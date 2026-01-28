This is an AI-powered data analysis application built with Streamlit that lets users interact with BigQuery databases using      
  natural language. It uses Google's Gemini LLM to convert plain English questions into SQL queries.                               
                                                                                                                                   
  Core Features                                                                                                                    
                                                                                                                                   
  1. Natural Language Chat - Ask questions in English, get SQL-generated answers                                                   
  2. Data Profiling - Generate statistical profiles of database tables                                                             
  3. Relationship Detection - Auto-detect and visualize table relationships                                                        
                                                                                                                                   
  Architecture                                                                                                                     
                                                                                                                                   
  app.py              → Main Streamlit entry point                                                                                 
  config/             → Settings and environment configuration                                                                     
  core/               → BigQuery client and SQL validation                                                                         
  llm/                → Gemini integration and text-to-SQL generation                                                              
  profiler/           → Table statistics and profiling engine                                                                      
  relationships/      → Relationship detection and graph visualization                                                             
  ui/                 → Streamlit pages and components                                                                             
  tests/              → Unit tests                                                                                                 
                                                                                                                                   
  Key Components                                                                                                                   
  ┌────────────────────────────┬──────────────────────────────────────────────────────┐                                            
  │           Module           │                       Purpose                        │                                            
  ├────────────────────────────┼──────────────────────────────────────────────────────┤                                            
  │ core/bigquery_client.py    │ BigQuery operations wrapper                          │                                            
  ├────────────────────────────┼──────────────────────────────────────────────────────┤                                            
  │ core/sql_validator.py      │ Security enforcement (SELECT-only queries)           │                                            
  ├────────────────────────────┼──────────────────────────────────────────────────────┤                                            
  │ llm/gemini_client.py       │ Vertex AI Gemini interface                           │                                            
  ├────────────────────────────┼──────────────────────────────────────────────────────┤                                            
  │ llm/sql_generator.py       │ Natural language → SQL conversion                    │                                            
  ├────────────────────────────┼──────────────────────────────────────────────────────┤                                            
  │ profiler/table_profiler.py │ Column statistics computation                        │                                            
  ├────────────────────────────┼──────────────────────────────────────────────────────┤                                            
  │ relationships/detector.py  │ Foreign key and pattern-based relationship detection │                                            
  ├────────────────────────────┼──────────────────────────────────────────────────────┤                                            
  │ ui/pages/                  │ Chatbot, profiling, and relationships pages          │                                            
  └────────────────────────────┴──────────────────────────────────────────────────────┘                                            
  Tech Stack                                                                                                                       
                                                                                                                                   
  - Streamlit - Web UI                                                                                                             
  - Google Cloud BigQuery - Data warehouse                                                                                         
  - Vertex AI Gemini - LLM for text-to-SQL                                                                                         
  - Plotly/NetworkX - Visualizations                                                                                               
  - sqlparse - SQL parsing and validation                                                                                          
                                                                                                                                   
  Security Features                                                                                                                
                                                                                                                                   
  - SQL validation blocks dangerous operations (INSERT, UPDATE, DELETE, DROP, etc.)                                                
  - Query byte limits to prevent runaway costs                                                                                     
  - SELECT-only query enforcement                                                                                                  
                                                                                                                                   
  To run: streamlit run app.py (requires Google Cloud authentication and environment configuration)  