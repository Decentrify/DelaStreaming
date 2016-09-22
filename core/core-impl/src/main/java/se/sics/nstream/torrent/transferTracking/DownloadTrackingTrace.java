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
package se.sics.nstream.torrent.transferTracking;

import java.util.List;
import se.sics.ledbat.core.DownloadThroughput;

/**
 *
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class DownloadTrackingTrace {

    public final DownloadThroughput throughput;
    public final double ongoingBlocks;
    public final double cwnd;

    public DownloadTrackingTrace(DownloadThroughput throughput, double ongoingBlocks, double cwnd) {
        this.throughput = throughput;
        this.ongoingBlocks = ongoingBlocks;
        this.cwnd = cwnd;
    }

    public static abstract class Accumulator {

        protected final DownloadTAccumulator throughput = new DownloadTAccumulator();
        protected double ongoingBlocks = 0;
        protected double cwnd = 0;
        protected int accumulated = 0;

        public void accumulate(DownloadTrackingTrace trace) {
            accumulated++;
            throughput.accumulate(trace.throughput);
            ongoingBlocks += trace.ongoingBlocks;
            cwnd += trace.cwnd;
        }

        public abstract DownloadTrackingTrace build();
    }

    public static class CrossConnectionAccumulator extends Accumulator {

        @Override
        public DownloadTrackingTrace build() {
            return new DownloadTrackingTrace(throughput.build(), ongoingBlocks, cwnd);
        }
    }

    public static class WithinConnectionAccumulator extends Accumulator {

        @Override
        public DownloadTrackingTrace build() {
            if (accumulated == 0) {
                return new DownloadTrackingTrace(throughput.build(), ongoingBlocks, cwnd);
            } else {
                return new DownloadTrackingTrace(throughput.build(), ongoingBlocks / accumulated, cwnd / accumulated);
            }
        }
        
        public void accumulate(List<DownloadTrackingTrace> traces) {
            for(DownloadTrackingTrace trace : traces) {
                accumulate(trace);
            }
        }
    }

    public static class DownloadTAccumulator {

        private double reqThroughput;
        private double inTimeThroughput;
        private double lateThroughput;
        private double timedOutThroughput;

        public DownloadTAccumulator() {
            reqThroughput = 0;
            inTimeThroughput = 0;
            lateThroughput = 0;
            timedOutThroughput = 0;
        }

        public DownloadTAccumulator accumulate(DownloadThroughput trace) {
            reqThroughput += trace.reqThroughput;
            inTimeThroughput += trace.inTimeThroughput;
            lateThroughput += trace.lateThroughput;
            timedOutThroughput += trace.timedOutThroughput;
            return this;
        }

        public DownloadThroughput build() {
            return new DownloadThroughput(reqThroughput, inTimeThroughput, lateThroughput, timedOutThroughput);
        }
    }
}
