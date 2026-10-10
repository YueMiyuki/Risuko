use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::{Arc, RwLock};

use crate::error::{Error, Result};

pub type Addrs = Box<dyn Iterator<Item = SocketAddr> + Send>;

pub type Resolving = Pin<Box<dyn Future<Output = Result<Addrs>> + Send>>;

pub trait Resolve: Send + Sync {
    fn resolve(&self, host: &str) -> Resolving;
}

#[derive(Clone, Default)]
pub struct GaiResolver;

impl Resolve for GaiResolver {
    fn resolve(&self, host: &str) -> Resolving {
        let host = host.to_string();
        Box::pin(async move {
            let addrs = tokio::net::lookup_host((host.as_str(), 0))
                .await
                .map_err(Error::Io)?;
            let v: Vec<SocketAddr> = addrs.collect();
            Ok(Box::new(v.into_iter()) as Addrs)
        })
    }
}

pub(crate) type SharedResolver = Arc<dyn Resolve>;

static GLOBAL_RESOLVER: RwLock<Option<SharedResolver>> = RwLock::new(None);

pub fn set_global_resolver(resolver: Option<SharedResolver>) {
    if let Ok(mut slot) = GLOBAL_RESOLVER.write() {
        *slot = resolver;
    }
}

fn global_resolver() -> Option<SharedResolver> {
    GLOBAL_RESOLVER.read().ok().and_then(|s| s.clone())
}

#[derive(Clone, Default)]
pub(crate) struct GlobalResolver;

impl Resolve for GlobalResolver {
    fn resolve(&self, host: &str) -> Resolving {
        match global_resolver() {
            Some(r) => r.resolve(host),
            None => GaiResolver.resolve(host),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[serial_test::serial]
    fn global_resolver_defaults_to_system() {
        set_global_resolver(None);
        assert!(global_resolver().is_none());
    }

    struct StaticResolver(SocketAddr);
    impl Resolve for StaticResolver {
        fn resolve(&self, _host: &str) -> Resolving {
            let a = self.0;
            Box::pin(async move { Ok(Box::new(std::iter::once(a)) as Addrs) })
        }
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn global_resolver_uses_override() {
        let addr: SocketAddr = "203.0.113.7:0".parse().unwrap();
        set_global_resolver(Some(Arc::new(StaticResolver(addr))));
        let got: Vec<SocketAddr> = GlobalResolver
            .resolve("anything.invalid")
            .await
            .unwrap()
            .collect();
        set_global_resolver(None);
        assert_eq!(got, vec![addr]);
    }
}
