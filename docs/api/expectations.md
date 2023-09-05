---
search:
  exclude: true
---

::: spark_expectations.core.expectations
    handler: python
    options:
        members:
            - SparkExpectations
            - DataframeNotReturnedExpection
            - SparkExpectOrFailException
        filters:
            - "!^_[^_]"
            - "!^__[^__]"
        