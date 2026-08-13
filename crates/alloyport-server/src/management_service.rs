//! Read-only operator API consumed by `alloyport-cli`.

use crate::WorkerControlService;
use alloyport_proto::management_v1::management_service_server::ManagementService;
use alloyport_proto::management_v1::{
    GetServerStatusRequest, ListWorkersRequest, ListWorkersResponse, ServerStatus, Worker,
};
use alloyport_proto::{PROTOCOL_MAJOR, PROTOCOL_MINOR};
use tonic::{Request, Response, Status};

#[derive(Clone)]
pub struct ManagementServiceImpl {
    control: WorkerControlService,
}

impl ManagementServiceImpl {
    #[must_use]
    pub const fn new(control: WorkerControlService) -> Self {
        Self { control }
    }
}

#[tonic::async_trait]
impl ManagementService for ManagementServiceImpl {
    async fn get_server_status(
        &self,
        _request: Request<GetServerStatusRequest>,
    ) -> Result<Response<ServerStatus>, Status> {
        let workers = self.control.worker_snapshots().await;
        Ok(Response::new(ServerStatus {
            server_version: env!("CARGO_PKG_VERSION").to_owned(),
            protocol_major: PROTOCOL_MAJOR,
            protocol_minor: PROTOCOL_MINOR,
            worker_count: workers.len() as u64,
            connected_worker_count: workers.iter().filter(|worker| worker.connected).count() as u64,
        }))
    }

    async fn list_workers(
        &self,
        _request: Request<ListWorkersRequest>,
    ) -> Result<Response<ListWorkersResponse>, Status> {
        let workers = self
            .control
            .worker_snapshots()
            .await
            .into_iter()
            .map(|snapshot| Worker {
                worker_id: snapshot.worker_id,
                instance_id: snapshot.instance_id,
                connected: snapshot.connected,
                last_worker_sequence: snapshot.last_worker_sequence,
                backend: snapshot.backend,
                features: snapshot.features,
            })
            .collect();
        Ok(Response::new(ListWorkersResponse { workers }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn reports_an_empty_but_healthy_daemon() -> Result<(), Status> {
        let service = ManagementServiceImpl::new(WorkerControlService::new());

        let status = service
            .get_server_status(Request::new(GetServerStatusRequest {}))
            .await?
            .into_inner();
        let workers = service
            .list_workers(Request::new(ListWorkersRequest {}))
            .await?
            .into_inner();

        assert_eq!(status.server_version, env!("CARGO_PKG_VERSION"));
        assert_eq!(status.protocol_major, PROTOCOL_MAJOR);
        assert_eq!(status.protocol_minor, PROTOCOL_MINOR);
        assert_eq!(status.worker_count, 0);
        assert_eq!(status.connected_worker_count, 0);
        assert!(workers.workers.is_empty());
        Ok(())
    }
}
