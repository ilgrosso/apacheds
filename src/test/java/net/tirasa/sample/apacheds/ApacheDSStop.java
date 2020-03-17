package net.tirasa.sample.apacheds;

import org.apache.directory.server.core.api.DirectoryService;
import org.apache.directory.server.ldap.LdapServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.stereotype.Component;

@Component
public class ApacheDSStop implements ApplicationListener<ContextClosedEvent> {

    private static final Logger LOG = LoggerFactory.getLogger(ApacheDSStop.class);

    @Override
    public void onApplicationEvent(final ContextClosedEvent event) {
        try {
            LdapServer server = event.getApplicationContext().getBean(LdapServer.class);
            if (server != null) {
                server.stop();
            }

            DirectoryService service = event.getApplicationContext().getBean(DirectoryService.class);
            if (service != null) {
                service.shutdown();
            }
        } catch (Exception e) {
            LOG.error("Fatal error in context shutdown", e);
            throw new RuntimeException(e);
        }
    }
}
