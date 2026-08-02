use serde::{Deserialize, Serialize};

use crate::id::{EventId, ExecutionId};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Event<T> {
    id: EventId,
    execution_id: ExecutionId,
    payload: T,
}

impl<T> Event<T> {
    pub fn new(execution_id: ExecutionId, payload: T) -> Self {
        Self {
            id: EventId::new(),
            execution_id,
            payload,
        }
    }

    pub fn with_id(id: EventId, execution_id: ExecutionId, payload: T) -> Self {
        Self {
            id,
            execution_id,
            payload,
        }
    }

    pub fn id(&self) -> EventId {
        self.id
    }

    pub fn execution_id(&self) -> ExecutionId {
        self.execution_id
    }

    pub fn payload(&self) -> &T {
        &self.payload
    }

    pub fn into_payload(self) -> T {
        self.payload
    }
}
