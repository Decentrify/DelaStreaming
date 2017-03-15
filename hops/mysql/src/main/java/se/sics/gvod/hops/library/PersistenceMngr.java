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

import java.util.Properties;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import se.sics.kompics.config.Config;

/**
 *
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class PersistenceMngr {

  public static EntityManagerFactory getEMF(Config config) {
    PersistenceConfig c = new PersistenceConfig(config);
    String mysqlURL = "jdbc:mysql://" + c.mysqlIp + ":" + c.mysqlPort + "/hopsworks";
    Properties props = new Properties();
    props.setProperty("javax.persistence.jdbc.url", mysqlURL);
    props.setProperty("javax.persistence.jdbc.user", c.mysqlUser);
    props.setProperty("javax.persistence.jdbc.password", c.mysqlPassword);
    props.setProperty("javax.persistence.jdbc.driver", "com.mysql.jdbc.Driver");
    props.setProperty("hibernate.show_sql", "true");
    props.setProperty("hibernate.format_sql", "true");
    props.setProperty("hibernate.dialect", "org.hibernate.dialect.MySQL5InnoDBDialect");
    props.setProperty("hibernate.hbm2ddl.auto", "validate");
    props.setProperty("hibernate.c3p0.min_size", "5");
    props.setProperty("hibernate.c3p0.max_size", "20");
    props.setProperty("hibernate.c3p0.timeout", "500");
    props.setProperty("hibernate.c3p0.max_statements", "50");
    props.setProperty("hibernate.c3p0.idle_test_period", "2000");
    
    return Persistence.createEntityManagerFactory("jpa-example", props);
  }
}
