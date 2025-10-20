// Copyright 2024 Google LLC

use protobuf::{reflect::MessageDescriptor, MessageDyn};
use std::{
    fmt,
    hash::{Hash, Hasher},
    time::Duration,
};

use crate::util::printable_small_duration;

#[cfg_attr(test, derive(Debug))]
pub struct PublisherConfig {
    pub service_name: String,
    pub loop_data: bool,
    pub parsed_data: Vec<ParsedDataEntry>,
    pub message_descriptor: MessageDescriptor,
}

impl Hash for PublisherConfig {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.service_name.hash(state);
    }
}

impl PartialEq for PublisherConfig {
    fn eq(&self, other: &Self) -> bool {
        self.service_name == other.service_name
    }
}

impl Eq for PublisherConfig {}

#[derive(Clone)]
#[cfg_attr(test, derive(Debug))]
pub struct ParsedDataEntry {
    pub message: Box<dyn MessageDyn>,
    pub message_bytes: Vec<u8>,
    pub delay: Duration,
}

impl fmt::Display for PublisherConfig {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "Configuration for publisher with service name \"{}\"", self.service_name)?;
        if self.loop_data {
            writeln!(f, "Data will be looped until the publisher subscription ends.")?;
        } else {
            writeln!(f, "Data will be replayed once.")?;
        }

        let num_messages = self.parsed_data.len();
        let max_displayed_messages = 20;
        writeln!(f, "Contains {num_messages} messages")?;
        if num_messages > max_displayed_messages {
            write!(f, ", showing the first {max_displayed_messages}")?;
        }
        for parsed_data_entry in self.parsed_data.iter().take(max_displayed_messages) {
            writeln!(f, "{parsed_data_entry}")?;
        }
        Ok(())
    }
}

impl fmt::Display for ParsedDataEntry {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "Delay: {}", printable_small_duration(&self.delay)?)?;
        writeln!(f, "{}", self.message)
    }
}
