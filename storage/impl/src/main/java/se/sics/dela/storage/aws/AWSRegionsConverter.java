/*
 * Copyright (C) 2009 Swedish Institute of Computer Science (SICS) Copyright (C)
 * 2009 Royal Institute of Technology (KTH)
 *
 * KompicsToolbox is free software; you can redistribute it and/or
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
package se.sics.dela.storage.aws;

import com.amazonaws.regions.Regions;
import se.sics.kompics.config.Converter;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class AWSRegionsConverter implements Converter<Regions> {

  @Override
  public Regions convert(Object o) {
    if (o instanceof String) {
      String reg = ((String) o).toLowerCase();
      reg = reg.replace("_", "-");
      Regions result = Regions.fromName(reg);
      return result;
    }
    return null;
  }

  @Override
  public Class type() {
    return Regions.class;
  }
}
