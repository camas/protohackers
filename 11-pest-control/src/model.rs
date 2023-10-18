#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SiteID(pub u32);

#[derive(Debug, Clone, Copy)]
pub struct PolicyID(pub u32);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpeciesAction {
    Cull,
    Conserve,
}
