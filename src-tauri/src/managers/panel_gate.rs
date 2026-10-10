#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
enum Phase {
    #[default]
    Absent,
    Building,
    Loading,
    Ready,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShowStep {
    Build,
    Wait,
    ShowNow,
}

#[derive(Debug, Default)]
pub struct PanelGate {
    phase: Phase,
    want_show: bool,
}

impl PanelGate {
    pub fn request_show(&mut self, window_exists: bool) -> ShowStep {
        if !window_exists && matches!(self.phase, Phase::Loading | Phase::Ready) {
            self.phase = Phase::Absent;
        }
        match self.phase {
            Phase::Absent => {
                self.phase = Phase::Building;
                self.want_show = true;
                ShowStep::Build
            }
            Phase::Building | Phase::Loading => {
                self.want_show = true;
                ShowStep::Wait
            }
            Phase::Ready => {
                self.want_show = false;
                ShowStep::ShowNow
            }
        }
    }

    pub fn cancel_pending(&mut self) -> bool {
        std::mem::take(&mut self.want_show)
    }

    pub fn built(&mut self, ok: bool) {
        if self.phase != Phase::Building {
            return;
        }
        if ok {
            self.phase = Phase::Loading;
        } else {
            self.phase = Phase::Absent;
            self.want_show = false;
        }
    }

    pub fn page_ready(&mut self) -> bool {
        self.phase = Phase::Ready;
        std::mem::take(&mut self.want_show)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn first_request_builds_then_shows_on_ready() {
        let mut g = PanelGate::default();
        assert_eq!(g.request_show(false), ShowStep::Build);
        g.built(true);
        assert!(g.page_ready());
        assert_eq!(g.request_show(true), ShowStep::ShowNow);
    }

    #[test]
    fn requests_during_build_or_load_wait() {
        let mut g = PanelGate::default();
        assert_eq!(g.request_show(false), ShowStep::Build);
        assert_eq!(g.request_show(false), ShowStep::Wait);
        g.built(true);
        assert_eq!(g.request_show(true), ShowStep::Wait);
        assert!(g.page_ready());
        assert!(!g.page_ready());
    }

    #[test]
    fn ready_before_built_is_not_downgraded() {
        let mut g = PanelGate::default();
        assert_eq!(g.request_show(false), ShowStep::Build);
        assert!(g.page_ready());
        g.built(true);
        assert_eq!(g.request_show(true), ShowStep::ShowNow);
    }

    #[test]
    fn cancel_drops_pending_show() {
        let mut g = PanelGate::default();
        g.request_show(false);
        g.built(true);
        assert!(g.cancel_pending());
        assert!(!g.cancel_pending());
        assert!(!g.page_ready());
    }

    #[test]
    fn failed_build_allows_retry() {
        let mut g = PanelGate::default();
        assert_eq!(g.request_show(false), ShowStep::Build);
        g.built(false);
        assert!(!g.cancel_pending());
        assert_eq!(g.request_show(false), ShowStep::Build);
    }

    #[test]
    fn destroyed_window_rebuilds() {
        let mut g = PanelGate::default();
        g.request_show(false);
        g.built(true);
        g.page_ready();
        assert_eq!(g.request_show(false), ShowStep::Build);
    }

    #[test]
    fn ready_without_request_does_not_show() {
        let mut g = PanelGate::default();
        g.request_show(false);
        g.built(true);
        g.cancel_pending();
        assert!(!g.page_ready());
    }
}
