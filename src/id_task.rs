use std::hash::{Hash, Hasher};

use futures::task::{self, Task};

#[derive(Clone)]
pub struct IdTask {
    pub task: Task,
    pub id: usize,
}

impl IdTask {
    pub fn new(task: Task, id: usize) -> IdTask {
        IdTask { task, id }
    }
}

impl Hash for IdTask {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state)
    }
}

impl PartialEq for IdTask {
    fn eq(&self, rhs: &IdTask) -> bool {
        self.id == rhs.id
    }
}

impl Eq for IdTask {}
