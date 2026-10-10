use crate::error::{Error, Result};
use url::Url;

pub trait IntoUrl: Sized {
    fn into_url(self) -> Result<Url>;
}

fn parse_url(s: &str) -> Result<Url> {
    Url::parse(s).map_err(|e| Error::Url(e.to_string()))
}

impl IntoUrl for Url {
    fn into_url(self) -> Result<Url> {
        Ok(self)
    }
}

impl IntoUrl for &Url {
    fn into_url(self) -> Result<Url> {
        Ok(self.clone())
    }
}

impl IntoUrl for &str {
    fn into_url(self) -> Result<Url> {
        parse_url(self)
    }
}

impl IntoUrl for &String {
    fn into_url(self) -> Result<Url> {
        parse_url(self)
    }
}

impl IntoUrl for String {
    fn into_url(self) -> Result<Url> {
        parse_url(&self)
    }
}
