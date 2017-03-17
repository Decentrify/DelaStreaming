/*
 * Copyright (C) 2009 Swedish Institute of Computer Science (SICS) Copyright (C)
 * 2009 Royal Institute of Technology (KTH)
 *
 * GVoD is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */
package se.sics.gvod.hops.library;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URL;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import javax.persistence.EntityManagerFactory;
import javax.persistence.SharedCacheMode;
import javax.persistence.ValidationMode;
import javax.persistence.spi.ClassTransformer;
import javax.persistence.spi.PersistenceUnitInfo;
import javax.persistence.spi.PersistenceUnitTransactionType;
import javax.sql.DataSource;
import org.hibernate.cfg.AvailableSettings;
import org.hibernate.jpa.HibernatePersistenceProvider;
import se.sics.gvod.hops.library.dao.TorrentDAO;
import se.sics.kompics.config.Config;

/**
 *
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class PersistenceMngr {

  public static EntityManagerFactory getEMF(Config config) {
    PersistenceConfig c = new PersistenceConfig(config);
    String mysqlURL = "jdbc:mysql://" + c.mysqlIp + ":" + c.mysqlPort + "/hopsworks";
//    Properties props = new Properties();
//    props.setProperty("javax.persistence.jdbc.url", mysqlURL);
//    props.setProperty("javax.persistence.jdbc.user", c.mysqlUser);
//    props.setProperty("javax.persistence.jdbc.password", c.mysqlPassword);
//    props.setProperty("javax.persistence.provider", "org.hibernate.jpa.HibernatePersistenceProvider");
//    props.setProperty("javax.persistence.jdbc.driver", "com.mysql.jdbc.Driver");
//    props.setProperty("hibernate.show_sql", "true");
//    props.setProperty("hibernate.format_sql", "true");
//    props.setProperty("hibernate.dialect", "org.hibernate.dialect.MySQL5InnoDBDialect");
//    props.setProperty("hibernate.hbm2ddl.auto", "validate");
//    props.setProperty("hibernate.c3p0.min_size", "5");
//    props.setProperty("hibernate.c3p0.max_size", "20");
//    props.setProperty("hibernate.c3p0.timeout", "500");
//    props.setProperty("hibernate.c3p0.max_statements", "50");
//    props.setProperty("hibernate.c3p0.idle_test_period", "2000");
//
//    PersistenceProvider provider = new HibernatePersistenceProvider();
//    EntityManagerFactory emf = provider.createEntityManagerFactory("dela-hops", props);

    EntityManagerFactory emf = new HibernatePersistenceProvider().createContainerEntityManagerFactory(
      myPUI(),
      ImmutableMap.<String, Object>builder()
      .put(AvailableSettings.DRIVER, "com.mysql.jdbc.Driver")
      .put(AvailableSettings.URL, mysqlURL)
      .put(AvailableSettings.USER, c.mysqlUser)
      .put(AvailableSettings.PASS, c.mysqlPassword)
      .put(AvailableSettings.DIALECT, "org.hibernate.dialect.MySQL5InnoDBDialect")
      .put(AvailableSettings.HBM2DDL_AUTO, "validate")
      .put(AvailableSettings.SHOW_SQL, "true")
      .put(AvailableSettings.FORMAT_SQL, "true")
      .put(AvailableSettings.C3P0_MIN_SIZE, "5")
      .put(AvailableSettings.C3P0_MAX_SIZE, "20")
      .put(AvailableSettings.C3P0_TIMEOUT, "0")
      .put(AvailableSettings.C3P0_MAX_STATEMENTS, "50")
      .put(AvailableSettings.C3P0_IDLE_TEST_PERIOD, "600")
      .build());
    return emf;
  }

  private static PersistenceUnitInfo myPUI() {
    return new PersistenceUnitInfo() {
      @Override
      public String getPersistenceUnitName() {
        return "ApplicationPersistenceUnit";
      }

      @Override
      public String getPersistenceProviderClassName() {
        return "org.hibernate.jpa.HibernatePersistenceProvider";
      }

      @Override
      public PersistenceUnitTransactionType getTransactionType() {
        return PersistenceUnitTransactionType.RESOURCE_LOCAL;
      }

      @Override
      public DataSource getJtaDataSource() {
        return null;
      }

      @Override
      public DataSource getNonJtaDataSource() {
        return null;
      }

      @Override
      public List<String> getMappingFileNames() {
        return Collections.emptyList();
      }

      @Override
      public List<URL> getJarFileUrls() {
        try {
          return Collections.list(this.getClass().getClassLoader().getResources(""));
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
      }

      @Override
      public URL getPersistenceUnitRootUrl() {
        return null;
      }

      @Override
      public List<String> getManagedClassNames() {
        List<String> managedClasses = new LinkedList<>();
        managedClasses.add(TorrentDAO.class.getCanonicalName());
        return managedClasses;
      }

      @Override
      public boolean excludeUnlistedClasses() {
        return false;
      }

      @Override
      public SharedCacheMode getSharedCacheMode() {
        return null;
      }

      @Override
      public ValidationMode getValidationMode() {
        return null;
      }

      @Override
      public Properties getProperties() {
        return new Properties();
      }

      @Override
      public String getPersistenceXMLSchemaVersion() {
        return null;
      }

      @Override
      public ClassLoader getClassLoader() {
        return null;
      }

      @Override
      public void addTransformer(ClassTransformer transformer) {

      }

      @Override
      public ClassLoader getNewTempClassLoader() {
        return null;
      }
    };
  }
}
