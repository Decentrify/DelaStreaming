/*
 * Copyright (C) 2013 - 2018, Logical Clocks AB and RISE SICS AB. All rights reserved
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this
 * software and associated documentation files (the "Software"), to deal in the Software
 * without restriction, including without limitation the rights to use, copy, modify, merge,
 * publish, distribute, sublicense, and/or sell copies of the Software, and to permit
 * persons to whom the Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or
 * substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS  OR IMPLIED, INCLUDING
 * BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL  THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
 * DAMAGES OR  OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 *
 */
package se.sics.dela.storage.impl;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import java.io.IOException;
import org.slf4j.LoggerFactory;
import se.sics.dela.storage.aws.AWSHelper;
import static se.sics.dela.storage.aws.AWSHelper.credentialsFromCSV;
import static se.sics.dela.storage.aws.AWSHelper.testCredentials;
import se.sics.ktoolbox.util.trysf.Try;
import static se.sics.dela.storage.aws.AWSHelper.client;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class AWSTester {
  public static void main(String[] args) throws IOException, InterruptedException {
    Try<AWSCredentials> creds = credentialsFromCSV("/Users/Alex/Documents/_Work/Cloud/AWS/dela.csv");
    AmazonS3 client = client(creds.get(), Regions.EU_WEST_1);
    testCredentials(client, LoggerFactory.getLogger(AWSHelper.class));
  }
}
