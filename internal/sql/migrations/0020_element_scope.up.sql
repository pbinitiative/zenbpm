-- What an element instance needs while a token stays in it across several jobs: where it stands
-- and the variables local to it. Written only for elements which carry execution listeners, when
-- the token first waits in the element; deleted when the token leaves the element or is taken out
-- of it, and with the history of the instance at the latest.
CREATE TABLE IF NOT EXISTS element_scope(
    key INTEGER PRIMARY KEY, -- int64 key of the element instance, as in flow_element_instance and job.element_instance_key
    process_instance_key INTEGER NOT NULL, -- int64 reference to process instance
    phase INTEGER NOT NULL, -- pkg/bpmn/runtime/element_scope.go:ElementPhase
    listener_index INTEGER NOT NULL DEFAULT 0, -- position, among the listeners of the phase, of the one whose job is next or waits
    input_variables TEXT NOT NULL, -- serialized json results of the element's input mappings
    local_variables TEXT NOT NULL, -- serialized json variables the jobs of the element were completed with
    FOREIGN KEY (process_instance_key) REFERENCES process_instance(key) -- reference to process instance
);
CREATE INDEX IF NOT EXISTS idx_fk_element_scope_process_instance_key ON element_scope(process_instance_key);
