 //Copyright 2021-2023 WHTCORPS INC
 //
 // Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 // this file except in compliance with the License. You may obtain a copy of the
 // License at http://www.apache.org/licenses/LICENSE-2.0
 // Unless required by applicable law or agreed to in writing, software distributed
 // under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 // CONDITIONS OF ANY KIND, either express or implied. See the License for the
 // specific language governing permissions and limitations under the License.

use super::Result;
use crate::store::SplitCheckTask;

use configuration::{ConfigChange, ConfigManager, Configuration};
use EinsteinDB_util::config::ReadableSize;
use EinsteinDB_util::worker::Scheduler;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Configuration)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct Config {
    /// When it is true, it will try to split a brane with table prefix if
    /// that brane crosses tables.
    pub split_brane_on_table: bool,

    /// For once split check, there are several split_key produced for batch.
    /// batch_split_limit limits the number of produced split-key for one batch.
    pub batch_split_limit: u64,

    /// When brane [a,e) size meets brane_max_size, it will be split into
    /// several branes [a,b), [b,c), [c,d), [d,e). And the size of [a,b),
    /// [b,c), [c,d) will be brane_split_size (maybe a little larger).
    pub brane_max_size: ReadableSize,
    pub brane_split_size: ReadableSize,

    /// When the number of keys in brane [a,e) meets the brane_max_keys,
    /// it will be split into two several branes [a,b), [b,c), [c,d), [d,e).
    /// And the number of keys in [a,b), [b,c), [c,d) will be brane_split_keys.
    pub brane_max_keys: u64,
    pub brane_split_keys: u64,
}

/// Default brane split size.
pub const SPLIT_SIZE_MB: u64 = 96;
/// Default brane split keys.
pub const SPLIT_KEYS: u64 = 960000;
/// Default batch split limit.
pub const BATCH_SPLIT_LIMIT: u64 = 10;

impl Default for Config {
    fn default() -> Config {
        let split_size = ReadableSize::mb(SPLIT_SIZE_MB);
        Config {
            split_brane_on_table: false,
            batch_split_limit: BATCH_SPLIT_LIMIT,
            brane_split_size: split_size,
            brane_max_size: split_size / 2 * 3,
            brane_split_keys: SPLIT_KEYS,
            brane_max_keys: SPLIT_KEYS / 2 * 3,
        }
    }
}

impl Config {
    pub fn validate(&self) -> Result<()> {
        if self.brane_max_size.0 < self.brane_split_size.0 {
            return Err(box_err!(
                "brane max size {} must >= split size {}",
                self.brane_max_size.0,
                self.brane_split_size.0
            ));
        }
        if self.brane_max_keys < self.brane_split_keys {
            return Err(box_err!(
                "brane max keys {} must >= split keys {}",
                self.brane_max_keys,
                self.brane_split_keys
            ));
        }
        Ok(())
    }
}

pub struct SplitCheckConfigManager(pub Scheduler<SplitCheckTask>);

impl ConfigManager for SplitCheckConfigManager {
    fn dispatch(
        &mut self,
        change: ConfigChange,
    ) -> std::result::Result<(), Box<dyn std::error::Error>> {
        self.0.schedule(SplitCheckTask::ChangeConfig(change))?;
        Ok(())
    }
}

impl std::ops::Deref for SplitCheckConfigManager {
    type Target = Scheduler<SplitCheckTask>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[brane(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_validate() {
        let mut brane = Config::default();
        brane.validate().unwrap();

        brane = Config::default();
        brane.brane_max_size = ReadableSize(10);
        brane.brane_split_size = ReadableSize(20);
        assert!(brane.validate().is_err());

        braneg = Config::default();
        brane.brane_max_keys = 10;
        brane.brane_split_keys = 20;
        assert!(brane.validate().is_err());
    }
}
