mod engine;

use axum::{
    Json, Router,
    extract::{Path, State, rejection::JsonRejection},
    http::StatusCode,
    routing::{get, post},
};
use engine::{Engine, WriteError};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{env, error::Error, path::PathBuf, sync::Arc};
use tokio::sync::Mutex;

#[derive(Serialize)]
struct Health {
    status: &'static str,
}

#[derive(Serialize)]
struct Version {
    name: &'static str,
    version: &'static str,
}

async fn health() -> Json<Health> {
    Json(Health { status: "ok" })
}

async fn version() -> Json<Version> {
    Json(Version {
        name: env!("CARGO_PKG_NAME"),
        version: env!("CARGO_PKG_VERSION"),
    })
}

struct AppState {
    engine: Mutex<Engine>,
}

#[derive(Deserialize)]
struct WriteRequest {
    records: Vec<Value>,
}

#[derive(Serialize)]
struct WriteResponse {
    collection: String,
    written: usize,
}

#[derive(Serialize)]
struct SaveResponse {
    version: String,
    saved_at: String,
}

#[derive(Serialize)]
struct QueryResponse {
    version: String,
    collection: String,
    records: Vec<Value>,
}

#[derive(Serialize)]
struct ErrorBody {
    error: String,
}

type ApiError = (StatusCode, Json<ErrorBody>);

fn api_error(status: StatusCode, message: impl Into<String>) -> ApiError {
    (
        status,
        Json(ErrorBody {
            error: message.into(),
        }),
    )
}

fn valid_collection_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 64
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}

async fn write_records(
    State(state): State<Arc<AppState>>,
    Path(collection): Path<String>,
    body: Result<Json<WriteRequest>, JsonRejection>,
) -> Result<Json<WriteResponse>, ApiError> {
    if !valid_collection_name(&collection) {
        return Err(api_error(
            StatusCode::BAD_REQUEST,
            format!("invalid collection name '{collection}'"),
        ));
    }
    let Json(request) = body.map_err(|e| {
        api_error(
            StatusCode::BAD_REQUEST,
            format!("invalid request body: {e}"),
        )
    })?;

    let mut engine = state.engine.lock().await;
    match engine.write_batch(&collection, request.records) {
        Ok(written) => Ok(Json(WriteResponse {
            collection,
            written,
        })),
        Err(WriteError::Invalid(message)) => Err(api_error(StatusCode::BAD_REQUEST, message)),
        Err(WriteError::Io(error)) => Err(api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("failed to persist batch: {error}"),
        )),
    }
}

async fn save_version(
    State(state): State<Arc<AppState>>,
) -> Result<(StatusCode, Json<SaveResponse>), ApiError> {
    let mut engine = state.engine.lock().await;
    let version = engine.save_version().map_err(|e| {
        api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("failed to persist version: {e}"),
        )
    })?;
    Ok((
        StatusCode::CREATED,
        Json(SaveResponse {
            version: version.id,
            saved_at: version.saved_at,
        }),
    ))
}

async fn query_version(
    State(state): State<Arc<AppState>>,
    Path((version, collection)): Path<(String, String)>,
) -> Result<Json<QueryResponse>, ApiError> {
    let engine = state.engine.lock().await;
    let Some(snapshot) = engine.version(&version) else {
        return Err(api_error(
            StatusCode::NOT_FOUND,
            format!("version '{version}' not found"),
        ));
    };
    let Some(records) = snapshot.collections.get(&collection) else {
        return Err(api_error(
            StatusCode::NOT_FOUND,
            format!("collection '{collection}' not found in version '{version}'"),
        ));
    };
    Ok(Json(QueryResponse {
        version,
        collection,
        records: records.clone(),
    }))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let bind = env::var("VDE_BIND").unwrap_or_else(|_| "127.0.0.1:8080".to_owned());
    let data_dir = env::var("VDE_DATA_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("data"));
    let engine = Engine::load(data_dir)?;
    println!("Data directory: {}", engine.dir().display());
    let state = Arc::new(AppState {
        engine: Mutex::new(engine),
    });

    let listener = tokio::net::TcpListener::bind(&bind).await?;
    let app = Router::new()
        .route("/health", get(health))
        .route("/version", get(version))
        .route("/collections/{collection}/records", post(write_records))
        .route("/versions", post(save_version))
        .route(
            "/versions/{version}/collections/{collection}/records",
            get(query_version),
        )
        .with_state(state);

    println!("Listening on http://{}", listener.local_addr()?);
    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    Ok(())
}
