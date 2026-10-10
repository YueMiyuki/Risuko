use super::core::Id20;

#[derive(Debug, Clone, Copy)]
pub enum TorrentIdOrHash {
    Id(usize),
    Hash(Id20),
}
