# Changelog

All notable release changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

For release details and downloads, visit the [GitHub Releases](https://github.com/Nike-Inc/spark-expectations/releases) page.

## [2.7.1] - 2025-11-13

**Spark Expectations v2.7.1 is here!!**

### NEW FEATURES

- **PagerDuty integration** - Alert on critical failures ([#194](https://github.com/Nike-Inc/spark-expectations/pull/194), [#205](https://github.com/Nike-Inc/spark-expectations/pull/205), [#214](https://github.com/Nike-Inc/spark-expectations/pull/214), [#225](https://github.com/Nike-Inc/spark-expectations/pull/225), [#227](https://github.com/Nike-Inc/spark-expectations/pull/227))
- **Priority-based notifications** - Control what triggers alerts ([#190](https://github.com/Nike-Inc/spark-expectations/pull/190), [#216](https://github.com/Nike-Inc/spark-expectations/pull/216), [#217](https://github.com/Nike-Inc/spark-expectations/pull/217))
- **Custom email templates** - Brand your notifications ([#178](https://github.com/Nike-Inc/spark-expectations/pull/178))
- **Rule validation** - Catch errors before execution ([#167](https://github.com/Nike-Inc/spark-expectations/pull/167))
- **Streaming support (Phase 1)** - Real-time DQ foundation ([#223](https://github.com/Nike-Inc/spark-expectations/pull/223))
- **Parameterized Kafka topics** - Better multi-env support ([#231](https://github.com/Nike-Inc/spark-expectations/pull/231))
- **Enhanced configuration support** - Use SparkConf/RuntimeConf ([#174](https://github.com/Nike-Inc/spark-expectations/pull/174), [#191](https://github.com/Nike-Inc/spark-expectations/pull/191))

### FIXES

- Fixed SQL DQ composite queries ([#207](https://github.com/Nike-Inc/spark-expectations/pull/207))
- Optimized rule retrieval performance - Changed from toLocalIterator() to collect() ([#198](https://github.com/Nike-Inc/spark-expectations/pull/198))
- Backward compatible priority column - Defaults to 0 if missing ([#201](https://github.com/Nike-Inc/spark-expectations/pull/201))
- Fixed Jinja template loading in packaged distribution ([#180](https://github.com/Nike-Inc/spark-expectations/pull/180))
- Fixed type checking in validate_rules ([#200](https://github.com/Nike-Inc/spark-expectations/pull/200))
- Added missing Kafka startup scripts ([#187](https://github.com/Nike-Inc/spark-expectations/pull/187))
- Added missing sqlglot library dependency ([#179](https://github.com/Nike-Inc/spark-expectations/pull/179))
- Fixed GitHub Pages deployment workflow ([#209](https://github.com/Nike-Inc/spark-expectations/pull/209), [#210](https://github.com/Nike-Inc/spark-expectations/pull/210), [#212](https://github.com/Nike-Inc/spark-expectations/pull/212))
- Fixed Codecov upload issues ([#197](https://github.com/Nike-Inc/spark-expectations/pull/197), [#199](https://github.com/Nike-Inc/spark-expectations/pull/199))
- Updated GitHub Actions workflow versions ([#213](https://github.com/Nike-Inc/spark-expectations/pull/213))
- Added validation for empty rule sets ([#181](https://github.com/Nike-Inc/spark-expectations/pull/181))

### IMPROVEMENTS

- Split unit and integration tests ([#183](https://github.com/Nike-Inc/spark-expectations/pull/183))
- Major project structure reorganization ([#222](https://github.com/Nike-Inc/spark-expectations/pull/222))
- Local test environment with JupyterLab and Mailpit containers ([#180](https://github.com/Nike-Inc/spark-expectations/pull/180))

### DOCUMENTATION

- New user guide https://engineering.nike.com/spark-expectations/latest/ ([#188](https://github.com/Nike-Inc/spark-expectations/pull/188))
- PagerDuty & Slack integration examples ([#205](https://github.com/Nike-Inc/spark-expectations/pull/205), [#215](https://github.com/Nike-Inc/spark-expectations/pull/215), [#218](https://github.com/Nike-Inc/spark-expectations/pull/218), [#227](https://github.com/Nike-Inc/spark-expectations/pull/227))
- Notification integration demo notebook ([#226](https://github.com/Nike-Inc/spark-expectations/pull/226))
- Updated documentation tooling dependencies ([#192](https://github.com/Nike-Inc/spark-expectations/pull/192))
- GitHub Copilot prompts for development ([#202](https://github.com/Nike-Inc/spark-expectations/pull/202))
- Fixed various documentation issues ([#233](https://github.com/Nike-Inc/spark-expectations/pull/233))

### UPGRADE NOTES

**Fully backward compatible - no breaking changes!**

#### Optional Enhancements

To enable priority-based notifications, add the priority column to your rules table:

```sql
ALTER TABLE rules ADD COLUMN priority INT DEFAULT 0;
```

---

[2.7.1]: https://github.com/Nike-Inc/spark-expectations/releases/tag/v2.7.1
