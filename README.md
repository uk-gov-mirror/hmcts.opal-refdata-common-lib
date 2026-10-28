# opal-opal-refdata-common-lib
## DTO generation

The DTO generator is defined in a standalone Gradle build file. Run it with:

```bash
./gradlew -b generate-ref-data.gradle generateRefDataDtos
```

It generates DTOs from `src/main/resources/jsonSchemas/ref-data/ref_data_schema.json` under `build/generated/sources/refDataDtos/main`.
