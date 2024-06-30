#[cfg(test)]
mod test_helpers {
    use crate::helpers::{
        convert_increment_to_precision, invert_side, read_settings, write_to_csv, SettingsFile, Side
    };
    use rust_decimal::prelude::FromPrimitive;
    use rust_decimal::Decimal;
    use csv::Reader;
    use std::fs;
    use std::fs::File;
    use serde_json::to_writer_pretty;

    #[test]
    fn test_convert_increment_to_precision() {
        let value = FromPrimitive::from_f64(0.1).unwrap();
        let precision = convert_increment_to_precision(value);
        assert_eq!(precision, 1u32);
    }

    #[test]
    fn test_write_to_csv() {
        let filename = "test_write_to_csv.csv";
        
        write_to_csv(
            filename,
            Decimal::from(10i64),
            Decimal::from(10i64),
            &Side::Sell,
            1usize,
        ).expect("Failed to write to CSV");

        let mut rdr = Reader::from_path(filename).expect("Failed to read CSV file");
        for result in rdr.records() {
            let record = result.expect("Failed to read record");
            assert_eq!(record[1], "10");
            assert_eq!(record[2], "10");
        }

        fs::remove_file(filename).expect("Failed to remove test file");
    }

    #[test]
    fn test_read_settings() {
        let filename = "test_read_settings.json";
        let data = SettingsFile {
            market_name: "BTC-USD".to_string(),
            time_delta: 1,
            bb_period: 10,
            bb_std_dev: 0.0,
            orderbook_depth: 0,
            live: false,
            order_size: Default::default(),
            tp_percent: Default::default(),
            sl_percent: Default::default(),
            write_to_file: false,
        };

        to_writer_pretty(&File::create(filename).expect("Failed to create file"), &data)
            .expect("Failed to write JSON data");

        let settings = read_settings(filename);
        assert_eq!(settings.time_delta, 1u64);
        assert_eq!(settings.bb_period, 10usize);
        assert_eq!(settings.bb_std_dev, 0f64);
        assert_eq!(settings.orderbook_depth, 0u32);

        fs::remove_file(filename).expect("Failed to remove test file");
    }

    #[test]
    fn test_invert_side() {
        assert_eq!(invert_side(Side::Sell), Side::Buy);
        assert_eq!(invert_side(Side::Buy), Side::Sell);
    }
}
