use tokio::sync::oneshot;

pub struct Response {
    pub msg: String,
}

pub struct Request {
    pub command: String,
    pub resp_tx: oneshot::Sender<Response>,
}
