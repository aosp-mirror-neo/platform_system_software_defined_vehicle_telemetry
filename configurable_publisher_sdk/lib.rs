// Copyright 2025 Google LLC

//! This module implements an SDK for the Configurable Publisher Registry service

/// Introduces Configurable Publisher trait that will be implemented by the client
pub mod configurable_publisher;

/// Contains functions that allows client to register and unregister configurable publishers
pub mod configurable_publisher_registry;

/// Contains traits that manage configurable publisher subscriptions
pub mod configurable_publisher_subscription;

/// Contains struct that stores information about the publisher being passed
pub mod publisher_info;

/// Contains Subscriber trait
pub mod subscriber;
