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
 * along with this program; if not, append to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */
package se.sics.dela.storage.common;

import java.io.IOException;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Supplier;
import se.sics.dela.storage.StorageEndpoint;
import se.sics.dela.storage.StorageResource;
import static se.sics.dela.storage.common.DelaHelper.recoverFrom;
import se.sics.ktoolbox.util.trysf.Try;
import static se.sics.ktoolbox.util.trysf.TryHelper.tryFSucc1;
import static se.sics.ktoolbox.util.trysf.TryHelper.tryTSucc0;
import static se.sics.ktoolbox.util.trysf.TryHelper.tryTSucc1;
import static se.sics.ktoolbox.util.trysf.TryHelper.Joiner;
import se.sics.nstream.hops.manifest.ManifestHelper;
import se.sics.nstream.hops.manifest.ManifestJSON;
import se.sics.nstream.util.range.KBlockImpl;
import se.sics.nstream.util.range.KRange;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class DelaStorageHelper<E extends StorageEndpoint, R extends StorageResource> {

  private final DelaStorageHandler<E, R> storage;

  public DelaStorageHelper(DelaStorageHandler storage) {
    this.storage = storage;
  }

  private static class ReadOpCollector {

    private Optional<Supplier<Try<Boolean>>> cleanup = Optional.empty();
    private Optional<Consumer<Try<ManifestJSON>>> resultCallback = Optional.empty();
    private Optional<Try<ManifestJSON>> manifestResult = Optional.empty();

    public void readCompleted(Try<byte[]> result) {
      Try<ManifestJSON> tryManifest = result
        .map(tryFSucc1((byte[] manifestBytes) -> ManifestHelper.getManifestJSON(manifestBytes)));
      this.manifestResult = Optional.of(tryManifest);
      processResult();
    }

    public void onResult(Supplier<Try<Boolean>> cleanup, Consumer<Try<ManifestJSON>> callback) {
      this.cleanup = Optional.of(cleanup);
      this.resultCallback = Optional.of(callback);
      processResult();
    }

    public void processResult() {
      if (cleanup.isPresent() && resultCallback.isPresent() && manifestResult.isPresent()) {
        Try<Boolean> cleanupResult = cleanup.get().get();
        if (manifestResult.get().isFailure()) {
          //in case of manifest error
          resultCallback.get().accept(manifestResult.get());
        } else if (cleanupResult.isFailure()) {
          //in case of cleanup error
          resultCallback.get().accept((Try.Failure) cleanupResult);
        } else {
          //if all is success
          resultCallback.get().accept(manifestResult.get());
        }
      }
    }

    public Consumer<Try<byte[]>> readCallback() {
      return (Try<byte[]> result) -> readCompleted(result);
    }
  }

  public void readManifest(StorageType storageType, E endpoint, R resource, long manifestSize,
    Consumer<Try<ManifestJSON>> callback) {
    ReadOpCollector collector = new ReadOpCollector();
    KRange readRange = new KBlockImpl(-1, 0, manifestSize);
    Try<DelaReadStream> readStream = new Try.Success(true)
      .flatMap(tryTSucc0(() -> storage.get(resource)))
      .flatMap(tryTSucc1((DelaFileHandler<E, R> file) ->
        file.readStream()));
    readStream
      .map(tryFSucc1((DelaReadStream stream) -> {
        stream.read(readRange, collector.readCallback());
        return true;
      }));
    Supplier<Try<Boolean>> cleanup = () ->
      readStream.flatMap(tryTSucc1((DelaReadStream stream) -> {
        try {
          stream.close();
          return new Try.Success(true);
        } catch (IOException ex) {
          String msg = "manifest close";
          return new Try.Failure(new DelaStorageException(msg, ex, storageType));
        }
      }));
    Consumer<Try<ManifestJSON>> finalResult = (Try<ManifestJSON> manifest) -> callback.accept(manifest);
    collector.onResult(cleanup, finalResult);
  }

  private static class AppendOpCollector {

    private Optional<Supplier<Try<Boolean>>> cleanup = Optional.empty();
    private Optional<Consumer<Try<Boolean>>> resultCallback = Optional.empty();
    private Optional<Try<Boolean>> writeResult = Optional.empty();
    private Optional<Try<Boolean>> completedResult = Optional.empty();

    public void writeCompleted(Try<Boolean> result) {
      if (writeResult.isPresent()) {
        writeResult = Optional.of(Joiner.map(writeResult.get(), result));
      } else {
        writeResult = Optional.of(result);
      }
    }

    public void completed(Try<Boolean> result) {
      completedResult = Optional.of(result);
      processResult();
    }

    public void onResult(Supplier<Try<Boolean>> cleanup, Consumer<Try<Boolean>> callback) {
      this.cleanup = Optional.of(cleanup);
      this.resultCallback = Optional.of(callback);
      processResult();
    }

    public void processResult() {
      if (cleanup.isPresent() && resultCallback.isPresent() && writeResult.isPresent() && completedResult.isPresent()) {
        Try<Boolean> cleanupResult = cleanup.get().get();
        resultCallback.get().accept(Joiner.map(writeResult.get(), completedResult.get(), cleanupResult));
      }
    }

    public Consumer<Try<Boolean>> stepCallback() {
      return (Try<Boolean> result) -> writeCompleted(result);
    }

    public Consumer<Try<Boolean>> completeCallback() {
      return (Try<Boolean> result) -> completed(result);
    }
  }

  public void writeManifest(StorageType storageType, E endpoint, R resource, ManifestJSON manifest,
    Consumer<Try<Boolean>> callback) {
    byte[] manifestBytes = ManifestHelper.getManifestByte(manifest);
    long appendPos = 0;
    long appendSize = manifestBytes.length;
    AppendOpCollector collector = new AppendOpCollector();
    Try<DelaAppendStream> appendStream = new Try.Success(true)
      .flatMap(tryTSucc0(() -> storage.get(resource)))
      .transform(failManifestExists(storageType, resource), rCreateManifest(storageType, endpoint, resource))
      .flatMap(tryTSucc1((DelaFileHandler<E, R> file) ->
        file.appendStream(appendSize, collector.completeCallback())));
    appendStream
      .map(tryFSucc1((DelaAppendStream stream) -> {
        stream.write(appendPos, manifestBytes, collector.stepCallback());
        return stream;
      }));
    Supplier<Try<Boolean>> cleanup = () ->
      appendStream.flatMap(tryTSucc1((DelaAppendStream stream) -> {
        try {
          stream.close();
          return new Try.Success(true);
        } catch (IOException ex) {
          String msg = "manifest close";
          return new Try.Failure(new DelaStorageException(msg, ex, storageType));
        }
      }));
    Consumer<Try<Boolean>> finalResult = (Try<Boolean> result) -> callback.accept(result);
    collector.onResult(cleanup, finalResult);
  }

  private <O> BiFunction<DelaFileHandler, Throwable, Try<O>> failManifestExists(StorageType storageType, R resource) {
    return tryTSucc1((DelaFileHandler file) -> {
      String msg = "manifest already exists:" + file.getResource().getSinkName();
      return new Try.Failure(new DelaStorageException(msg, storageType));
    });
  }

  private BiFunction<DelaFileHandler<E, R>, Throwable, Try<DelaFileHandler<E, R>>> rCreateManifest(
    StorageType storageType, E endpoint, R resource) {
    return recoverFrom(storageType, DelaStorageException.RESOURCE_DOES_NOT_EXIST,
      () -> storage.create(resource));
  }
}
