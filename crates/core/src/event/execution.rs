#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExecutionCreated;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExecutionStarted;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExecutionCompleted;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExecutionFailed;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExecutionCancelled;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExecutionTerminated;
