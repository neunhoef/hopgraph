use clap::{Parser, Subcommand};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::time::{Duration, Instant};
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
        /// Number of times to run the query (default: 5)
        #[arg(long, default_value = "5")]
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

async fn run_hop_query(client: &ArangoClient, iterations: usize) -> Result<()> {
    let query = r#"
        FOR a IN A
          FOR b IN 1..1 OUTBOUND a._id E
            FOR c IN 1..1 OUTBOUND b._id F
              LIMIT 1000
              RETURN {a, b, c}
    "#;

    println!("Running hop query {iterations} times...");
    println!("Query:\n{query}");

    let mut total_duration = Duration::new(0, 0);
    let mut results_count = 0;

    for i in 1..=iterations {
        print!("Iteration {i}/{iterations}: ");
        use std::io::{self, Write};
        io::stdout().flush().unwrap();
        
        let (result, duration) = client.execute_aql_query(query).await?;
        total_duration += duration;
        
        if let Some(count) = result.get("count") {
            results_count = count.as_u64().unwrap_or(0);
        }
        
        println!("{}ms (returned {} results)", duration.as_millis(), results_count);
    }

    let avg_duration = total_duration / iterations as u32;
    println!("\nQuery Performance Summary:");
    println!("  Total iterations: {iterations}");
    println!("  Total time: {}ms", total_duration.as_millis());
    println!("  Average time: {}ms", avg_duration.as_millis());
    println!("  Results per query: {results_count}");

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
            run_hop_query(&client, iterations).await?;
        }
    }

    Ok(())
}
