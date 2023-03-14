use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CensusZone {
    // England: OA11CD
    pub id: String,

    // (England-only for now)
    // 0 cars or vars per household. See https://www.ons.gov.uk/datasets/TS045/editions/2021/versions/3 for details.
    pub cars_0: u16,
    pub cars_1: u16,
    pub cars_2: u16,
    // 3 or more cars or vans per household
    pub cars_3: u16,
}

impl CensusZone {
    // Assumes "3 or more" just means 3
    pub fn total_cars(&self) -> u16 {
        self.cars_1 + 2 * self.cars_2 + 3 * self.cars_3
    }
}
