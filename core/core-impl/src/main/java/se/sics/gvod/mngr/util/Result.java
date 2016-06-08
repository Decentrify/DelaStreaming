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
package se.sics.gvod.mngr.util;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class Result {
    public static enum Status {
        SUCCESS, TIMEOUT, INTERNAL_FAILURE, BAD_REQUEST;
    }
    
    public final Status status;
    public final String description;
    
    public Result(Status status, String description) {
        this.status = status;
        this.description = description;
    }
    
    public static Result success() {
        return new Result(Status.SUCCESS, "success");
    }
    
    public static Result badRequest(String description) {
        return new Result(Status.BAD_REQUEST, description);
    }
    
    public static Result fail(String description) {
        return new Result(Status.INTERNAL_FAILURE, description);
    }
}
