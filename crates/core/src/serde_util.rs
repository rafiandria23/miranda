pub mod duration_secs_opt {
    use serde::{Deserialize, Deserializer, Serializer};
    use std::time::Duration;

    pub fn serialize<S: Serializer>(value: &Option<Duration>, s: S) -> Result<S::Ok, S::Error> {
        match value {
            Some(d) => s.serialize_some(&d.as_secs()),
            None => s.serialize_none(),
        }
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Option<Duration>, D::Error> {
        let secs: Option<u64> = Option::deserialize(d)?;

        Ok(secs.map(Duration::from_secs))
    }

    // =========================================================================
    // Testing
    // =========================================================================

    #[cfg(test)]
    mod tests {
        use super::*;

        #[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
        struct Wrapper {
            #[serde(default, with = "super")]
            value: Option<Duration>,
        }

        #[test]
        fn serializes_some_duration_as_seconds() {
            let wrapper = Wrapper {
                value: Some(Duration::from_secs(42)),
            };

            let json = serde_json::to_value(&wrapper).unwrap();

            assert_eq!(json["value"], 42);
        }

        #[test]
        fn serializes_zero_duration_as_zero() {
            let wrapper = Wrapper {
                value: Some(Duration::from_secs(0)),
            };

            let json = serde_json::to_value(&wrapper).unwrap();

            assert_eq!(json["value"], 0);
        }

        #[test]
        fn serializes_none_as_null() {
            let wrapper = Wrapper { value: None };

            let json = serde_json::to_value(&wrapper).unwrap();

            assert_eq!(json["value"], serde_json::Value::Null);
        }

        #[test]
        fn truncates_sub_second_precision_on_serialize() {
            let wrapper = Wrapper {
                value: Some(Duration::from_millis(1_500)),
            };

            let json = serde_json::to_value(&wrapper).unwrap();

            assert_eq!(json["value"], 1);
        }

        #[test]
        fn deserializes_seconds_into_some_duration() {
            let wrapper: Wrapper = serde_json::from_str(r#"{"value":42}"#).unwrap();

            assert_eq!(wrapper.value, Some(Duration::from_secs(42)));
        }

        #[test]
        fn deserializes_zero_into_some_zero_duration() {
            let wrapper: Wrapper = serde_json::from_str(r#"{"value":0}"#).unwrap();

            assert_eq!(wrapper.value, Some(Duration::from_secs(0)));
        }

        #[test]
        fn deserializes_large_value_into_some_duration() {
            let wrapper: Wrapper = serde_json::from_str(r#"{"value":31536000}"#).unwrap();

            assert_eq!(wrapper.value, Some(Duration::from_secs(31_536_000)));
        }

        #[test]
        fn deserializes_null_into_none() {
            let wrapper: Wrapper = serde_json::from_str(r#"{"value":null}"#).unwrap();

            assert_eq!(wrapper.value, None);
        }

        #[test]
        fn deserializes_missing_field_into_none() {
            let wrapper: Wrapper = serde_json::from_str(r#"{}"#).unwrap();

            assert_eq!(wrapper.value, None);
        }

        #[test]
        fn rejects_negative_seconds() {
            let result: Result<Wrapper, _> = serde_json::from_str(r#"{"value":-1}"#);

            assert!(result.is_err());
        }

        #[test]
        fn rejects_non_integer_seconds() {
            let result: Result<Wrapper, _> = serde_json::from_str(r#"{"value":"42"}"#);

            assert!(result.is_err());
        }

        #[test]
        fn round_trips_through_serialize_and_deserialize() {
            let original = Wrapper {
                value: Some(Duration::from_secs(123)),
            };

            let json = serde_json::to_string(&original).unwrap();
            let round_tripped: Wrapper = serde_json::from_str(&json).unwrap();

            assert_eq!(original, round_tripped);
        }
    }
}
