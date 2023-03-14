use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize)]
pub struct OutputArea {
    pub oa11cd: String,

    // 0 cars or vars per household. See https://www.ons.gov.uk/datasets/TS045/editions/2021/versions/3 for details.
    pub cars_0: u16,
    pub cars_1: u16,
    pub cars_2: u16,
    // 3 or more cars or vans per household
    pub cars_3: u16,
}
