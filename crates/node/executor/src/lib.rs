//! Block execution abstractions and REVM-based implementation for Kora.

#![doc = include_str!("../README.md")]
#![doc(issue_tracker_base_url = "https://github.com/refcell/kora/issues/")]
#![cfg_attr(docsrs, feature(doc_cfg, doc_auto_cfg))]
#![cfg_attr(not(test), warn(unused_crate_dependencies))]

mod adapter;
pub use adapter::StateDbAdapter;

mod config;
pub use config::{BaseFeeParams, ExecutionConfig, GasLimitBounds};

mod context;
pub use context::{BlockContext, ParentBlock};

mod error;
pub use error::ExecutionError;

mod outcome;
pub use outcome::{ExecutionOutcome, ExecutionReceipt};

mod revm;
pub use revm::{CallParams, RevmExecutor, calculate_base_fee};

mod traits;
pub use traits::BlockExecutor;
