# Resources
The initial schema creation and schema updates are managed by Flyway: [https://flywaydb.org/documentation/migrations]
To install Flyway command line tools go to: [https://flywaydb.org/documentation/commandline/#download-and-installation]

# Migration modes
The schema migration can be handled automatically during the bootstrap process, or via command line. To enable
bootstrap time migration set `titus.ext.jooq.activity.createSchemaIfNotExist` to true.

# Manual update procedure

Example for dev stack:
* install the Flyway toolkit ([https://flywaydb.org/documentation/commandline/#download-and-installation])
* go to the schema folder `titus-control-plane/titus-ext/jooq-activity-queue/src/main/resources/db/migration/activity`
* run `flyway migrate -user=<user> -password=<password> -url=jdbc:postgresql://<dev_rds_host>:5432/titus -locations=filesystem:.`
