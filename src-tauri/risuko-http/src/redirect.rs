use url::Url;

#[derive(Clone, Debug)]
pub struct Policy {
    pub(crate) max: usize,
    pub(crate) allow: Option<fn(&Url) -> bool>,
}

impl Policy {
    pub fn limited(max: usize) -> Self {
        Self { max, allow: None }
    }

    pub fn allow_only(mut self, allow: fn(&Url) -> bool) -> Self {
        self.allow = Some(allow);
        self
    }
}

impl Default for Policy {
    fn default() -> Self {
        Self::limited(10)
    }
}
