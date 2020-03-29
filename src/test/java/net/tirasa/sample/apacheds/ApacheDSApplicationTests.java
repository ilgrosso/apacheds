package net.tirasa.sample.apacheds;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import org.apache.directory.api.ldap.model.cursor.CursorException;
import org.apache.directory.api.ldap.model.cursor.EntryCursor;
import org.apache.directory.api.ldap.model.entry.Entry;
import org.apache.directory.api.ldap.model.exception.LdapException;
import org.apache.directory.api.ldap.model.message.SearchScope;
import org.apache.directory.ldap.client.api.LdapConnection;
import org.apache.directory.ldap.client.api.LdapNetworkConnection;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
public class ApacheDSApplicationTests {

    @Value("${apacheds.port}")
    private int port;

    private LdapConnection connection;

    @BeforeEach
    public void init() throws LdapException {
        connection = new LdapNetworkConnection("localhost", port);
        connection.bind("uid=admin,ou=system", "secret");
    }

    @AfterEach
    public void cleanup() throws LdapException, IOException {
        connection.unBind();
        connection.close();
    }

    private void checkFound(final EntryCursor cursor) throws LdapException, CursorException, IOException {
        int found = 0;
        while (cursor.next()) {
            found++;
            Entry entry = cursor.get();
            assertEquals("cn=testLDAPGroup,ou=Groups,o=isp", entry.getDn().getName());
        }
        assertEquals(1, found);
    }

    // This finds the group as expected:
    // (&(objectClass=groupOfUniqueNames)(cn=testLDAPGroup))
    @Test
    public void success1() throws LdapException, CursorException, IOException {
        try (EntryCursor cursor = connection.search(
                "ou=groups,o=isp",
                "(&(objectClass=groupOfUniqueNames)(cn=testLDAPGroup))", SearchScope.ONELEVEL, "*")) {

            checkFound(cursor);
        }
    }

    // After https://github.com/apache/directory-server/commit/b8a5559 now this as well finds the group as expected:
    // (&(objectClass=top)(cn=testLDAPGroup))
    @Test
    public void success2() throws LdapException, CursorException, IOException {
        try (EntryCursor cursor = connection.search(
                "ou=groups,o=isp",
                "(&(objectClass=top)(cn=testLDAPGroup))", SearchScope.ONELEVEL, "*")) {

            checkFound(cursor);
        }
    }

    // After https://github.com/apache/directory-server/commit/b8a5559 now this as well finds the group as expected:
    // (&(&(objectClass=top)(objectClass=groupOfUniqueNames))(cn=testLDAPGroup))
    @Test
    public void success3() throws LdapException, CursorException, IOException {
        try (EntryCursor cursor = connection.search(
                "ou=groups,o=isp",
                "(&(&(objectClass=top)(objectClass=groupOfUniqueNames))(cn=testLDAPGroup))",
                SearchScope.ONELEVEL, "*")) {

            checkFound(cursor);
        }
    }
}
