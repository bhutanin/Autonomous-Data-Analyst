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
⏺ Tools & Technologies Used                                                                                                          
                                                                                                                                     
  Core Dependencies                                                                                                                  
  ┌─────────────────┬───────────────────────────┬─────────────────────────────────────┐                                              
  │    Category     │       Tool/Library        │               Purpose               │                                              
  ├─────────────────┼───────────────────────────┼─────────────────────────────────────┤                                              
  │ Web Framework   │ Streamlit                 │ Interactive UI and dashboard        │                                              
  ├─────────────────┼───────────────────────────┼─────────────────────────────────────┤                                              
  │ Data Warehouse  │ Google BigQuery           │ Data storage and SQL execution      │                                              
  ├─────────────────┼───────────────────────────┼─────────────────────────────────────┤                                              
  │ LLM             │ Google Gemini (Vertex AI) │ Natural language to SQL generation  │                                              
  ├─────────────────┼───────────────────────────┼─────────────────────────────────────┤                                              
  │ Data Processing │ Pandas, PyArrow           │ Data manipulation and serialization │                                              
  ├─────────────────┼───────────────────────────┼─────────────────────────────────────┤                                              
  │ SQL Parsing     │ sqlparse                  │ SQL validation and security         │                                              
  ├─────────────────┼───────────────────────────┼─────────────────────────────────────┤                                              
  │ Visualization   │ Plotly                    │ Interactive charts and graphs       │                                              
  ├─────────────────┼───────────────────────────┼─────────────────────────────────────┤                                              
  │ Graph Analysis  │ NetworkX                  │ Relationship detection and analysis │                                              
  ├─────────────────┼───────────────────────────┼─────────────────────────────────────┤                                              
  │ Configuration   │ python-dotenv             │ Environment variable management     │                                              
  └─────────────────┴───────────────────────────┴─────────────────────────────────────┘                                              
  External Services                                                                                                                  
                                                                                                                                     
  - Google Cloud BigQuery - Data warehouse backend                                                                                   
  - Vertex AI Gemini API - LLM for text-to-SQL conversion                                                                            
                                                                                                                                     
  ---                                                                                                                                
  Architecture Diagram                                                                                                               
                                                                                                                                     
  ┌─────────────────────────────────────────────────────────────────────────────┐                                                    
  │                              USER INTERFACE                                  │                                                   
  │                           (Streamlit - app.py)                              │                                                    
  │  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────────────────┐  │                                                    
  │  │   Chat Page     │  │  Profiling Page │  │    Relationships Page       │  │                                                    
  │  │  (chatbot.py)   │  │(data_profiling) │  │   (relationships.py)        │  │                                                    
  │  └────────┬────────┘  └────────┬────────┘  └─────────────┬───────────────┘  │                                                    
  └───────────┼────────────────────┼────────────────────────┼───────────────────┘                                                    
              │                    │                        │                                                                        
              ▼                    ▼                        ▼                                                                        
  ┌───────────────────────────────────────────────────────────────────────────────┐                                                  
  │                          SESSION MANAGER (ui/session_manager.py)              │                                                  
  │         [Conversation History] [Schema Cache] [Profile Cache] [Rel Cache]     │                                                  
  └───────────────────────────────────────────────────────────────────────────────┘                                                  
              │                    │                        │                                                                        
              ▼                    ▼                        ▼                                                                        
  ┌─────────────────────┐ ┌─────────────────────┐ ┌─────────────────────────────┐                                                    
  │    LLM LAYER        │ │   PROFILER LAYER    │ │    RELATIONSHIPS LAYER      │                                                    
  │      (llm/)         │ │     (profiler/)     │ │      (relationships/)       │                                                    
  │                     │ │                     │ │                             │                                                    
  │ ┌─────────────────┐ │ │ ┌─────────────────┐ │ │ ┌─────────────────────────┐ │                                                    
  │ │  SQLGenerator   │ │ │ │ TableProfiler   │ │ │ │ RelationshipDetector    │ │                                                    
  │ │  - generate_sql │ │ │ │ - profile_table │ │ │ │ - detect_relationships  │ │                                                    
  │ │  - retry logic  │ │ │ │ - column stats  │ │ │ │ - validate_relationship │ │                                                    
  │ └────────┬────────┘ │ │ └────────┬────────┘ │ │ └───────────┬─────────────┘ │                                                    
  │          │          │ │          │          │ │             │               │                                                    
  │ ┌────────▼────────┐ │ │ ┌────────▼────────┐ │ │ ┌───────────▼─────────────┐ │                                                    
  │ │ PromptTemplates │ │ │ │ ColumnStats     │ │ │ │ ColumnMatcher           │ │                                                    
  │ │ - system instr  │ │ │ │ - numeric stats │ │ │ │ - pattern matching      │ │                                                    
  │ │ - text-to-sql   │ │ │ │ - string stats  │ │ │ │ - type compatibility    │ │                                                    
  │ └─────────────────┘ │ │ │ - top values    │ │ │ │ - confidence scoring    │ │                                                    
  │                     │ │ └─────────────────┘ │ │ └─────────────────────────┘ │                                                    
  │ ┌─────────────────┐ │ │                     │ │                             │                                                    
  │ │SchemaContext    │ │ │                     │ │ ┌─────────────────────────┐ │                                                    
  │ │Builder          │ │ │                     │ │ │ RelationshipGraphBuilder│ │                                                    
  │ │ - format schema │ │ │                     │ │ │ - NetworkX graph        │ │                                                    
  │ │ - extract tables│ │ │                     │ │ │ - Plotly visualization  │ │                                                    
  │ └─────────────────┘ │ │                     │ │ └─────────────────────────┘ │                                                    
  └──────────┬──────────┘ └──────────┬──────────┘ └──────────────┬──────────────┘                                                    
             │                       │                           │                                                                   
             ▼                       ▼                           ▼                                                                   
  ┌─────────────────────────────────────────────────────────────────────────────┐                                                    
  │                           CORE LAYER (core/)                                 │                                                   
  │  ┌─────────────────────────────────────────────────────────────────────┐    │                                                    
  │  │                      BigQueryClient                                  │    │                                                   
  │  │  - list_datasets()  - execute_query()  - get_table_schema()         │    │                                                    
  │  │  - list_tables()    - validate_query_syntax()                       │    │                                                    
  │  └─────────────────────────────────────────────────────────────────────┘    │                                                    
  │  ┌─────────────────────────┐  ┌────────────────────────────────────────┐    │                                                    
  │  │     SQLValidator        │  │           Settings (config/)           │    │                                                    
  │  │  - SELECT-only enforce  │  │  - project_id, dataset, model          │    │                                                    
  │  │  - block DML/DDL        │  │  - query limits, thresholds            │    │                                                    
  │  └─────────────────────────┘  └────────────────────────────────────────┘    │                                                    
  └──────────────────────────────────┬──────────────────────────────────────────┘                                                    
                                     │                                                                                               
             ┌───────────────────────┴───────────────────────┐                                                                       
             ▼                                               ▼                                                                       
  ┌─────────────────────────────┐             ┌─────────────────────────────────┐                                                    
  │      GEMINI CLIENT          │             │         BIGQUERY API            │                                                    
  │      (llm/gemini_client)    │             │                                 │                                                    
  │                             │             │  - Query execution              │                                                    
  │  - generate() (single-turn) │             │  - Schema introspection         │                                                    
  │  - chat() (multi-turn)      │             │  - INFORMATION_SCHEMA           │                                                    
  │  - extract_sql()            │             │  - Cost controls                │                                                    
  └──────────────┬──────────────┘             └────────────────┬────────────────┘                                                    
                 │                                             │                                                                     
                 ▼                                             ▼                                                                     
  ┌─────────────────────────────────────────────────────────────────────────────┐                                                    
  │                         GOOGLE CLOUD PLATFORM                                │                                                   
  │         ┌─────────────────────┐       ┌─────────────────────────┐           │                                                    
  │         │    Vertex AI        │       │      BigQuery           │           │                                                    
  │         │   Gemini 2.0 Flash  │       │   (Data Warehouse)      │           │                                                    
  │         └─────────────────────┘       └─────────────────────────┘           │                                                    
  └─────────────────────────────────────────────────────────────────────────────┘                                                    
                                                                                                                                     
  ---                                                                                                                                
  Data Flow                                                                                                                          
                                                                                                                                     
  Chat Query Flow                                                                                                                    
                                                                                                                                     
  User Question → SessionManager (get history) → SQLGenerator                                                                        
                                                      ↓                                                                              
                                SchemaContextBuilder (format schema)                                                                 
                                                      ↓                                                                              
                                PromptTemplates (build prompt)                                                                       
                                                      ↓                                                                              
                                GeminiClient.generate() → LLM                                                                        
                                                      ↓                                                                              
                                Extract SQL from response                                                                            
                                                      ↓                                                                              
                                SQLValidator (security check)                                                                        
                                                      ↓                                                                              
                                BigQueryClient.execute_query()                                                                       
                                                      ↓                                                                              
                                Results → SessionManager → UI                                                                        
                                                                                                                                     
  Key Design Patterns                                                                                                                
  ┌────────────────────┬────────────────────────────┬─────────────────────┐                                                          
  │      Pattern       │           Usage            │       Benefit       │                                                          
  ├────────────────────┼────────────────────────────┼─────────────────────┤                                                          
  │ Lazy Loading       │ BigQuery/Gemini clients    │ Faster startup      │                                                          
  ├────────────────────┼────────────────────────────┼─────────────────────┤                                                          
  │ LRU Caching        │ Settings, schemas          │ Single instance     │                                                          
  ├────────────────────┼────────────────────────────┼─────────────────────┤                                                          
  │ Retry with Backoff │ SQL generation (3 retries) │ Error recovery      │                                                          
  ├────────────────────┼────────────────────────────┼─────────────────────┤                                                          
  │ Dataclasses        │ TableProfile, Relationship │ Type safety         │                                                          
  ├────────────────────┼────────────────────────────┼─────────────────────┤                                                          
  │ Security Layers    │ SQLValidator + dry-run     │ Defense in depth    │                                                          
  ├────────────────────┼────────────────────────────┼─────────────────────┤                                                          
  │ Confidence Scoring │ Relationship matching      │ Flexible thresholds │                                                          
  └────────────────────┴────────────────────────────┴─────────────────────┘                                                                                                                                         
  To run: streamlit run app.py (requires Google Cloud authentication and environment configuration)  