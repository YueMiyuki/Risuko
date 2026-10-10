#[derive(Clone, Debug)]
pub struct Policy {
    pub(crate) max: usize,
}

impl Policy {
    pub fn limited(max: usize) -> Self {
        Self { max }
    }
}

impl Default for Policy {
    fn default() -> Self {
        Self::limited(10)
    }
}
