package net.tirasa.sample.apacheds;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.directory.api.ldap.model.constants.SchemaConstants;
import org.apache.directory.api.ldap.model.entry.Entry;
import org.apache.directory.api.ldap.model.name.Dn;
import org.apache.directory.api.ldap.model.schema.LdapComparator;
import org.apache.directory.api.ldap.model.schema.SchemaManager;
import org.apache.directory.api.ldap.model.schema.comparators.NormalizingComparator;
import org.apache.directory.api.ldap.model.schema.registries.ComparatorRegistry;
import org.apache.directory.api.ldap.model.schema.registries.SchemaLoader;
import org.apache.directory.api.ldap.schema.extractor.SchemaLdifExtractor;
import org.apache.directory.api.ldap.schema.extractor.impl.DefaultSchemaLdifExtractor;
import org.apache.directory.api.ldap.schema.loader.LdifSchemaLoader;
import org.apache.directory.api.ldap.schema.manager.impl.DefaultSchemaManager;
import org.apache.directory.api.util.exception.Exceptions;
import org.apache.directory.server.constants.ServerDNConstants;
import org.apache.directory.server.core.DefaultDirectoryService;
import org.apache.directory.server.core.api.DirectoryService;
import org.apache.directory.server.core.api.DnFactory;
import org.apache.directory.server.core.api.InstanceLayout;
import org.apache.directory.server.core.api.partition.Partition;
import org.apache.directory.server.core.api.schema.SchemaPartition;
import org.apache.directory.server.core.factory.JdbmPartitionFactory;
import org.apache.directory.server.core.partition.impl.btree.jdbm.JdbmIndex;
import org.apache.directory.server.core.partition.impl.btree.jdbm.JdbmPartition;
import org.apache.directory.server.core.partition.ldif.LdifPartition;
import org.apache.directory.server.i18n.I18n;
import org.apache.directory.server.ldap.LdapServer;
import org.apache.directory.server.protocol.shared.transport.TcpTransport;
import org.apache.directory.server.xdbm.Index;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationListener;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;

@Component
public class ApacheDSStart implements ApplicationListener<ContextRefreshedEvent> {

    private static final Logger LOG = LoggerFactory.getLogger(ApacheDSStart.class);

    /**
     * Add a new partition to the server.
     *
     * @param service service
     * @param partitionId The partition Id
     * @param partitionDn The partition DN
     * @param dnFactory the DN factory
     * @return The newly added partition
     * @throws Exception If the partition can't be added
     */
    private void addPartition(
            final DirectoryService service,
            final String partitionId,
            final String partitionDn,
            final DnFactory dnFactory)
            throws Exception {

        // Create a new partition with the given partition id
        JdbmPartition partition = new JdbmPartition(service.getSchemaManager(), dnFactory);
        partition.setId(partitionId);
        partition.setPartitionPath(new File(service.getInstanceLayout().getPartitionsDirectory(), partitionId).toURI());
        partition.setSuffixDn(new Dn(partitionDn));
        service.addPartition(partition);

        Set<Index<?, String>> indexedAttributes = Stream.of(
                SchemaConstants.OBJECT_CLASS_AT, SchemaConstants.OU_AT,
                SchemaConstants.UID_AT, SchemaConstants.CN_AT).
                map(attr -> new JdbmIndex<String>(attr, false)).collect(Collectors.toSet());
        partition.setIndexedAttributes(indexedAttributes);
    }

    /**
     * Initialize the schema manager and add the schema partition to directory service.
     *
     * @param service service
     * @throws Exception if the schema LDIF files are not found on the classpath
     */
    private void initSchemaPartition(final DirectoryService service) throws Exception {
        File workingDirectory = service.getInstanceLayout().getPartitionsDirectory();

        // Extract the schema on disk (a brand new one) and load the registries
        File schemaRepository = new File(workingDirectory, "schema");
        SchemaLdifExtractor extractor = new DefaultSchemaLdifExtractor(workingDirectory);
        try {
            extractor.extractOrCopy();
        } catch (IOException ioe) {
            // The schema has already been extracted, bypass
        }

        SchemaLoader loader = new LdifSchemaLoader(schemaRepository);
        SchemaManager schemaManager = new DefaultSchemaManager(loader);

        // We have to load the schema now, otherwise we won't be able
        // to initialize the Partitions, as we won't be able to parse 
        // and normalize their suffix Dn
        schemaManager.loadAllEnabled();

        // Tell all the normalizer comparators that they should not normalize anything
        ComparatorRegistry comparatorRegistry = schemaManager.getComparatorRegistry();
        for (LdapComparator<?> comparator : comparatorRegistry) {
            if (comparator instanceof NormalizingComparator) {
                ((NormalizingComparator) comparator).setOnServer();
            }
        }

        service.setSchemaManager(schemaManager);

        // Init the LdifPartition
        LdifPartition ldifPartition = new LdifPartition(schemaManager, service.getDnFactory());
        ldifPartition.setPartitionPath(new File(workingDirectory, "schema").toURI());
        SchemaPartition schemaPartition = new SchemaPartition(schemaManager);
        schemaPartition.setWrappedPartition(ldifPartition);
        service.setSchemaPartition(schemaPartition);

        List<Throwable> errors = schemaManager.getErrors();
        if (!errors.isEmpty()) {
            throw new IllegalStateException(I18n.err(I18n.ERR_317, Exceptions.printErrors(errors)));
        }
    }

    private void initSystemPartition(final DirectoryService service) throws Exception {
        JdbmPartitionFactory partitionFactory = new JdbmPartitionFactory();

        Partition systemPartition = partitionFactory.createPartition(
                service.getSchemaManager(),
                service.getDnFactory(),
                "system",
                ServerDNConstants.SYSTEM_DN,
                500,
                new File(service.getInstanceLayout().getPartitionsDirectory(), "system"));
        systemPartition.setSchemaManager(service.getSchemaManager());

        partitionFactory.addIndex(systemPartition, SchemaConstants.OBJECT_CLASS_AT, 100);

        service.setSystemPartition(systemPartition);
    }

    /**
     * Initialize the server. It creates the partition, adds the index, and injects the context entries for the created
     * partitions.
     *
     * @param workDir the directory to be used for storing the data
     * @param loadDefaultContent if default content should be loaded
     * @throws Exception if there were some problems while initializing
     */
    private DirectoryService initDirectoryService(
            final File workDir,
            final boolean loadDefaultContent) throws Exception {

        // Initialize the LDAP service
        DirectoryService service = new DefaultDirectoryService();
        service.setInstanceLayout(new InstanceLayout(workDir));

        // first load the schema
        initSchemaPartition(service);

        // then the system partition
        initSystemPartition(service);

        // Disable the ChangeLog system
        service.getChangeLog().setEnabled(false);
        service.setDenormalizeOpAttrsEnabled(true);

        // Now we can create as many partitions as we need
        addPartition(service, "isp", "o=isp", service.getDnFactory());

        // And start the service
        service.startup();

        if (loadDefaultContent) {
            ClassPathResource contentLdif = new ClassPathResource("/content.ldif");
            LdifInputStreamLoader contentLoader =
                    new LdifInputStreamLoader(service.getAdminSession(), contentLdif.getInputStream());
            int numEntries = contentLoader.execute();
            LOG.info("Successfully created {} entries", numEntries);
        }

        return service;
    }

    @Override
    public void onApplicationEvent(final ContextRefreshedEvent event) {
        Entry result;
        try {
            File workDir = Files.createTempDirectory("apacheds").toFile();
            DirectoryService service = initDirectoryService(workDir, true);
            ((ConfigurableApplicationContext) event.getApplicationContext()).
                    getBeanFactory().registerSingleton("service", service);

            LdapServer server = new LdapServer();
            server.setTransports(new TcpTransport(
                    event.getApplicationContext().getEnvironment().getProperty("apacheds.port", Integer.class)));
            server.setDirectoryService(service);
            ((ConfigurableApplicationContext) event.getApplicationContext()).
                    getBeanFactory().registerSingleton("server", server);

            server.start();

            result = service.getAdminSession().lookup(new Dn("o=isp"));
        } catch (Exception e) {
            LOG.error("Fatal error in context init", e);
            throw new RuntimeException(e);
        }

        if (result == null) {
            throw new RuntimeException("Base DN not found");
        } else {
            LOG.info("ApacheDS startup completed succesfully");
        }
    }
}
