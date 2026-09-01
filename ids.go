package durable

// PipelineID identifies a durable Pipeline.
type PipelineID string

// ResourceID identifies the logical resource a Run operates on.
type ResourceID string

// RunID identifies one exact execution of one Pipeline against one resource.
type RunID string

// StepID identifies durable Step semantics: forward behavior, unwind
// behavior, and Step State schema.
type StepID string
