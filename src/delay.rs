#[derive(Debug, Serialize, Deserialize)]
pub struct DelayMessage {
    #[serde(default)]
    pub correlation_id: Option<String>,
    pub message: String,
    pub topic_name: String
}