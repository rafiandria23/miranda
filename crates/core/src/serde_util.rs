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

    #[cfg(test)]
    mod tests {
        use super::*;

        #[derive(serde::Serialize, serde::Deserialize)]
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
        fn serializes_none_as_null() {
            let wrapper = Wrapper { value: None };

            let json = serde_json::to_value(&wrapper).unwrap();

            assert_eq!(json["value"], serde_json::Value::Null);
        }

        #[test]
        fn deserializes_seconds_into_some_duration() {
            let wrapper: Wrapper = serde_json::from_str(r#"{"value":42}"#).unwrap();

            assert_eq!(wrapper.value, Some(Duration::from_secs(42)));
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
    }
}
