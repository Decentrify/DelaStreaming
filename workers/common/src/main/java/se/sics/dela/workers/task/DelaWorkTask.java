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
package se.sics.dela.workers.task;

import se.sics.kompics.util.Identifier;
import se.sics.ktoolbox.nutil.conn.workers.WorkTask;
import se.sics.ktoolbox.util.network.KAddress;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class DelaWorkTask {

  public static abstract class Request implements WorkTask.Request {

    public Identifier taskId;
    public final Identifier dataId;

    public Request(Identifier taskId, Identifier dataId) {
      this.taskId = taskId;
      this.dataId = dataId;
    }

    @Override
    public Identifier taskId() {
      return taskId;
    }

    @Override
    public Result deadWorker() {
      return new Fail(taskId);
    }
  }

  public static class LReceiver extends Request {
    public final KAddress sender;
    
    public LReceiver(Identifier taskId, KAddress sender, Identifier dataId) {
      super(taskId, dataId);
      this.sender = sender;
    }
    
    @Override
    public String toString() {
      return "LReceiver{" + '}';
    }
  }

  public static class LSender extends Request {
    public final KAddress receiver;
    public LSender(Identifier taskId, KAddress receiver, Identifier dataId) {
      super(taskId, dataId);
      this.receiver = receiver;
    }

    @Override
    public String toString() {
      return "LSender{" + '}';
    }
  }

  public static class Status implements WorkTask.Status {

    public Identifier taskId;

    public Status(Identifier taskId) {
      this.taskId = taskId;
    }

    @Override
    public Identifier taskId() {
      return taskId;
    }
  }

  public static abstract class Result implements WorkTask.Result {

    public Identifier taskId;

    public Result(Identifier taskId) {
      this.taskId = taskId;
    }

    @Override
    public Identifier taskId() {
      return taskId;
    }
  }

  public static class Fail extends Result {

    public Fail(Identifier taskId) {
      super(taskId);
    }
  }

  public static class Success extends Result {

    public Success(Identifier taskId) {
      super(taskId);
    }
  }
}
