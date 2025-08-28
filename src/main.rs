use clap::{Parser, Subcommand};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::time::{Duration, Instant};
use std::path::Path;
use std::fs;
use anyhow::Result;
use rand::prelude::*;

#[derive(Parser)]
#[command(name = "hopgraph")]
#[command(about = "A tool for creating and querying ArangoDB hop graphs")]
struct Cli {
    /// ArangoDB endpoint URL
    #[arg(short, long, default_value = "http://localhost:8529")]
    endpoint: String,
    
    /// Username for authentication
    #[arg(short, long, default_value = "root")]
    username: String,
    
    /// Password for authentication
    #[arg(short, long, default_value = "")]
    password: String,
    
    /// Database name
    #[arg(short, long, default_value = "hopgraph")]
    database: String,
    
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Create the graph structure and populate with data
    Create {
        /// Number of documents per collection (default: 1000000)
        #[arg(long, default_value = "1000000")]
        num_docs: usize,
        
        /// Number of edges per edge collection (default: 900000)
        #[arg(long, default_value = "900000")]
        num_edges: usize,
        
        /// Batch size for insertions (default: 1000)
        #[arg(long, default_value = "1000")]
        batch_size: usize,
    },
    /// Execute queries on the graph
    Query {
        /// Number of times to run each query (default: 20)
        #[arg(long, default_value = "20")]
        iterations: usize,
    },
}

#[derive(Debug, Serialize, Deserialize)]
struct GraphDefinition {
    name: String,
    #[serde(rename = "edgeDefinitions")]
    edge_definitions: Vec<EdgeDefinition>,
    #[serde(rename = "orphanCollections")]
    orphan_collections: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct EdgeDefinition {
    collection: String,
    from: Vec<String>,
    to: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct Document {
    _key: String,
    payload: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct Edge {
    _key: String,
    _from: String,
    _to: String,
    payload: String,
}

#[derive(Debug)]
struct QueryStats {
    query_name: String,
    #[allow(dead_code)]
    query_content: String,
    runtimes: Vec<Duration>,
    result_count: u64,
}

impl QueryStats {
    fn new(query_name: String, query_content: String) -> Self {
        Self {
            query_name,
            query_content,
            runtimes: Vec::new(),
            result_count: 0,
        }
    }

    fn add_runtime(&mut self, duration: Duration, result_count: u64) {
        self.runtimes.push(duration);
        self.result_count = result_count; // Assuming result count is consistent across runs
    }

    fn calculate_statistics(&self) -> Option<StatsSummary> {
        if self.runtimes.is_empty() {
            return None;
        }

        let mut sorted_runtimes = self.runtimes.clone();
        sorted_runtimes.sort();

        let count = sorted_runtimes.len();
        let sum: Duration = sorted_runtimes.iter().sum();
        let average = sum / count as u32;
        
        let median = if count % 2 == 0 {
            (sorted_runtimes[count / 2 - 1] + sorted_runtimes[count / 2]) / 2
        } else {
            sorted_runtimes[count / 2]
        };

        let percentile_90_index = ((count as f64 * 0.90).ceil() as usize).saturating_sub(1);
        let percentile_90 = sorted_runtimes[percentile_90_index];

        let min = sorted_runtimes[0];
        let max = sorted_runtimes[count - 1];

        Some(StatsSummary {
            average,
            median,
            percentile_90,
            min,
            max,
        })
    }
}

#[derive(Debug)]
struct StatsSummary {
    average: Duration,
    median: Duration,
    percentile_90: Duration,
    min: Duration,
    max: Duration,
}

struct ArangoClient {
    client: Client,
    base_url: String,
    auth_header: String,
    database: String,
}

impl ArangoClient {
    fn new(endpoint: &str, username: &str, password: &str, database: &str) -> Self {
        use base64::{Engine as _, engine::general_purpose};
        let auth = general_purpose::STANDARD.encode(format!("{username}:{password}"));
        Self {
            client: Client::new(),
            base_url: endpoint.to_string(),
            auth_header: format!("Basic {auth}"),
            database: database.to_string(),
        }
    }

    async fn create_database(&self) -> Result<()> {
        let url = format!("{}//_api/database", self.base_url);
        let body = json!({
            "name": self.database
        });
        
        let response = self.client
            .post(&url)
            .header("Authorization", &self.auth_header)
            .json(&body)
            .send()
            .await?;
            
        if response.status().is_success() || response.status() == 409 {
            println!("Database '{}' ready", self.database);
            Ok(())
        } else {
            let text = response.text().await?;
            anyhow::bail!("Failed to create database: {}", text);
        }
    }

    async fn create_graph(&self, graph_def: &GraphDefinition) -> Result<()> {
        let url = format!("{}/_db/{}/_api/gharial", self.base_url, self.database);
        
        let response = self.client
            .post(&url)
            .header("Authorization", &self.auth_header)
            .json(graph_def)
            .send()
            .await?;
            
        if response.status().is_success() || response.status() == 409 {
            println!("Graph '{}' created successfully", graph_def.name);
            Ok(())
        } else {
            let text = response.text().await?;
            anyhow::bail!("Failed to create graph: {}", text);
        }
    }

    async fn insert_documents_batch(&self, collection: &str, documents: &[Document]) -> Result<()> {
        let url = format!("{}/_db/{}/_api/document/{}", self.base_url, self.database, collection);
        
        let response = self.client
            .post(&url)
            .header("Authorization", &self.auth_header)
            .json(documents)
            .send()
            .await?;
            
        if !response.status().is_success() {
            let text = response.text().await?;
            anyhow::bail!("Failed to insert documents into {}: {}", collection, text);
        }
        
        Ok(())
    }

    async fn insert_edges_batch(&self, collection: &str, edges: &[Edge]) -> Result<()> {
        let url = format!("{}/_db/{}/_api/document/{}", self.base_url, self.database, collection);
        
        let response = self.client
            .post(&url)
            .header("Authorization", &self.auth_header)
            .json(edges)
            .send()
            .await?;
            
        if !response.status().is_success() {
            let text = response.text().await?;
            anyhow::bail!("Failed to insert edges into {}: {}", collection, text);
        }
        
        Ok(())
    }

    async fn execute_aql_query(&self, query: &str) -> Result<(Value, Duration)> {
        let url = format!("{}/_db/{}/_api/cursor", self.base_url, self.database);
        let body = json!({
            "query": query,
            "count": true,
            "batchSize": 1000
        });
        
        let start = Instant::now();
        let response = self.client
            .post(&url)
            .header("Authorization", &self.auth_header)
            .json(&body)
            .send()
            .await?;
        let duration = start.elapsed();
            
        if response.status().is_success() {
            let result: Value = response.json().await?;
            Ok((result, duration))
        } else {
            let text = response.text().await?;
            anyhow::bail!("Failed to execute query: {}", text);
        }
    }
}

fn generate_random_string(length: usize) -> String {
    let mut rng = thread_rng();
    (0..length)
        .map(|_| rng.sample(rand::distributions::Alphanumeric) as char)
        .collect()
}

fn read_query_files(queries_dir: &str) -> Result<Vec<(String, String)>> {
    let path = Path::new(queries_dir);
    
    if !path.exists() {
        anyhow::bail!("Queries directory '{}' does not exist", queries_dir);
    }
    
    if !path.is_dir() {
        anyhow::bail!("Path '{}' is not a directory", queries_dir);
    }
    
    let mut queries = Vec::new();
    
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let file_path = entry.path();
        
        if let Some(extension) = file_path.extension() {
            if extension == "aql" {
                let file_name = file_path
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .unwrap_or("unknown")
                    .to_string();
                
                let content = fs::read_to_string(&file_path)
                    .map_err(|e| anyhow::anyhow!("Failed to read file {:?}: {}", file_path, e))?;
                
                queries.push((file_name, content.trim().to_string()));
            }
        }
    }
    
    if queries.is_empty() {
        anyhow::bail!("No .aql files found in directory '{}'", queries_dir);
    }
    
    // Sort queries by name for consistent output
    queries.sort_by(|a, b| a.0.cmp(&b.0));
    
    Ok(queries)
}

async fn create_hop_graph(
    client: &ArangoClient,
    num_docs: usize,
    num_edges: usize,
    batch_size: usize,
) -> Result<()> {
    println!("Creating database...");
    client.create_database().await?;

    // Define the graph structure
    let mut vertex_collections = vec!["A".to_string()];
    for i in 1..=9 {
        vertex_collections.push(format!("B{i}"));
        vertex_collections.push(format!("C{i}"));
    }

    let b_collections: Vec<String> = (1..=9).map(|i| format!("B{i}")).collect();
    let c_collections: Vec<String> = (1..=9).map(|i| format!("C{i}")).collect();

    let graph_def = GraphDefinition {
        name: "hopgraph".to_string(),
        edge_definitions: vec![
            EdgeDefinition {
                collection: "E".to_string(),
                from: vec!["A".to_string()],
                to: b_collections.clone(),
            },
            EdgeDefinition {
                collection: "F".to_string(),
                from: vec!["B1".to_string()],
                to: c_collections,
            },
        ],
        orphan_collections: vec![],
    };

    println!("Creating graph structure...");
    client.create_graph(&graph_def).await?;

    // Insert documents into vertex collections
    println!("Inserting documents into vertex collections...");
    for collection in &vertex_collections {
        println!("  Inserting {num_docs} documents into {collection}");
        
        for batch_start in (0..num_docs).step_by(batch_size) {
            let batch_end = (batch_start + batch_size).min(num_docs);
            let batch: Vec<Document> = (batch_start..batch_end)
                .map(|i| Document {
                    _key: format!("doc_{i}"),
                    payload: generate_random_string(100),
                })
                .collect();
                
            client.insert_documents_batch(collection, &batch).await?;
            
            if batch_start % 10000 == 0 {
                print!("    Progress: {batch_start}/{num_docs}\r");
                use std::io::{self, Write};
                io::stdout().flush().unwrap();
            }
        }
        println!("    Completed: {num_docs}/{num_docs}");
    }

    // Insert edges for collection E (A -> B1-B9)
    println!("Inserting {num_edges} edges into collection E...");
    let mut rng = thread_rng();
    
    for batch_start in (0..num_edges).step_by(batch_size) {
        let batch_end = (batch_start + batch_size).min(num_edges);
        let batch: Vec<Edge> = (batch_start..batch_end)
            .map(|i| {
                let a_doc = rng.gen_range(0..num_docs);
                let b_collection = rng.gen_range(1..=9);
                let b_doc = rng.gen_range(0..num_docs);
                
                Edge {
                    _key: format!("edge_{i}"),
                    _from: format!("A/doc_{a_doc}"),
                    _to: format!("B{b_collection}/doc_{b_doc}"),
                    payload: generate_random_string(50),
                }
            })
            .collect();
            
        client.insert_edges_batch("E", &batch).await?;
        
        if batch_start % 10000 == 0 {
            print!("    Progress: {batch_start}/{num_edges}\r");
            use std::io::{self, Write};
            io::stdout().flush().unwrap();
        }
    }
    println!("    Completed: {num_edges}/{num_edges}");

    // Insert edges for collection F (B1-B9 -> C1-C9)
    println!("Inserting {num_edges} edges into collection F...");
    
    for batch_start in (0..num_edges).step_by(batch_size) {
        let batch_end = (batch_start + batch_size).min(num_edges);
        let batch: Vec<Edge> = (batch_start..batch_end)
            .map(|i| {
                let b_doc = rng.gen_range(0..num_docs);
                let c_collection = rng.gen_range(1..=9);
                let c_doc = rng.gen_range(0..num_docs);
                
                Edge {
                    _key: format!("edge_{i}"),
                    _from: format!("B1/doc_{b_doc}"),
                    _to: format!("C{c_collection}/doc_{c_doc}"),
                    payload: generate_random_string(50),
                }
            })
            .collect();
            
        client.insert_edges_batch("F", &batch).await?;
        
        if batch_start % 10000 == 0 {
            print!("    Progress: {batch_start}/{num_edges}\r");
            use std::io::{self, Write};
            io::stdout().flush().unwrap();
        }
    }
    println!("    Completed: {num_edges}/{num_edges}");

    println!("Graph creation completed successfully!");
    Ok(())
}

async fn run_queries(client: &ArangoClient, iterations: usize) -> Result<()> {
    let queries = read_query_files("queries")?;
    
    println!("Found {} query file(s) in the queries directory", queries.len());
    println!("Running each query {iterations} times...\n");
    
    let mut all_stats = Vec::new();
    
    for (query_name, query_content) in queries {
        println!("🔍 Executing query: {query_name}");
        println!("Query content:\n{query_content}\n");
        
        let mut stats = QueryStats::new(query_name.clone(), query_content.clone());
        
        for i in 1..=iterations {
            print!("  Iteration {i}/{iterations}: ");
            use std::io::{self, Write};
            io::stdout().flush().unwrap();
            
            let (result, duration) = client.execute_aql_query(&query_content).await?;
            
            let result_count = result.get("count")
                .and_then(|c| c.as_u64())
                .unwrap_or(0);
            
            stats.add_runtime(duration, result_count);
            
            println!("{}ms ({} results)", duration.as_millis(), result_count);
        }
        
        all_stats.push(stats);
        println!(); // Empty line between queries
    }
    
    // Print comprehensive statistics
    println!("📊 QUERY PERFORMANCE SUMMARY");
    println!("{}", "=".repeat(80));
    
    for stats in &all_stats {
        if let Some(summary) = stats.calculate_statistics() {
            println!("\nQuery: {}", stats.query_name);
            println!("  Iterations: {}", stats.runtimes.len());
            println!("  Results per query: {}", stats.result_count);
            println!("  Average runtime: {}ms", summary.average.as_millis());
            println!("  Median runtime: {}ms", summary.median.as_millis());
            println!("  90th percentile: {}ms", summary.percentile_90.as_millis());
            println!("  Min runtime: {}ms", summary.min.as_millis());
            println!("  Max runtime: {}ms", summary.max.as_millis());
        }
    }
    
    // Overall summary if multiple queries
    if all_stats.len() > 1 {
        let total_queries: usize = all_stats.iter().map(|s| s.runtimes.len()).sum();
        let total_time: Duration = all_stats.iter()
            .flat_map(|s| &s.runtimes)
            .sum();
        
        println!("\n{}", "=".repeat(80));
        println!("OVERALL SUMMARY");
        println!("  Total query files: {}", all_stats.len());
        println!("  Total query executions: {total_queries}");
        println!("  Total execution time: {}ms", total_time.as_millis());
        println!("  Average time per execution: {}ms", 
                 (total_time / total_queries as u32).as_millis());
    }
    
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    
    let client = ArangoClient::new(
        &cli.endpoint,
        &cli.username,
        &cli.password,
        &cli.database,
    );

    match cli.command {
        Commands::Create { num_docs, num_edges, batch_size } => {
            let start = Instant::now();
            create_hop_graph(&client, num_docs, num_edges, batch_size).await?;
            println!("Total creation time: {}s", start.elapsed().as_secs());
        }
        Commands::Query { iterations } => {
            run_queries(&client, iterations).await?;
        }
    }

    Ok(())
}
