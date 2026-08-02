use crate::id::{EventId, ExecutionId};

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

    pub fn id(&self) -> EventId {
        self.id
    }

    pub fn execution_id(&self) -> ExecutionId {
        self.execution_id
    }

    pub fn payload(&self) -> &T {
        &self.payload
    }
}

#[cfg(test)]
mod tests {
    use crate::event::execution::ExecutionStarted;

    use super::*;

    #[test]
    fn creates_event() {
        let execution_id = ExecutionId::new();

        let event = Event::new(execution_id, ExecutionStarted);

        assert_eq!(event.execution_id(), execution_id);
        assert_eq!(*event.payload(), ExecutionStarted);
        assert_ne!(event.id(), EventId::new());
    }
}
