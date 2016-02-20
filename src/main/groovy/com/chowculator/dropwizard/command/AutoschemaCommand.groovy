package com.chowculator.dropwizard.command

import com.chowculator.cassandraschema.accessor.system.DescribeAccessor
import com.chowculator.cassandraschema.autoschema.AutoSchema
import com.chowculator.cassandraschema.autoschema.Cql
import com.chowculator.cassandraschema.connection.Connector
import com.chowculator.cassandraschema.connection.ConnectorConfig
import com.datastax.driver.mapping.MappingManager
import io.dropwizard.Configuration
import io.dropwizard.cli.ConfiguredCommand
import io.dropwizard.setup.Bootstrap
import net.sourceforge.argparse4j.inf.Namespace

abstract class AutoschemaCommand<T extends Configuration> extends ConfiguredCommand<T> {
    private final Class[] entities

    AutoschemaCommand(Class... entities) {
        super("autoschema", "Runs autoschema")
        this.entities = entities
    }

    abstract ConnectorConfig getConnectorConfig(T configuration)

    @Override
    void run(Bootstrap<T> bootstrap, Namespace namespace, T configuration) throws Exception {
        Connector connector = new Connector(getConnectorConfig(configuration))
        MappingManager mappingManager = new MappingManager(connector.session)
        DescribeAccessor describer = mappingManager.createAccessor(DescribeAccessor)
        Cql cql = new Cql(connector)
        AutoSchema autoSchema = new AutoSchema(describer, cql)

        autoSchema.generate(getConnectorConfig(configuration).keyspace, entities)

        connector.session.cluster.close()
    }
}