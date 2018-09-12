///*
// * Copyright (C) 2009 Swedish Institute of Computer Science (SICS) Copyright (C)
// * 2009 Royal Institute of Technology (KTH)
// *
// * KompicsToolbox is free software; you can redistribute it and/or
// * modify it under the terms of the GNU General Public License
// * as published by the Free Software Foundation; either version 2
// * of the License, or (at your option) any later version.
// *
// * This program is distributed in the hope that it will be useful,
// * but WITHOUT ANY WARRANTY; without even the implied warranty of
// * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// * GNU General Public License for more details.
// *
// * You should have received a copy of the GNU General Public License
// * along with this program; if not, write to the Free Software
// * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
// */
//package se.sics.dela.storage.cache;
//
//import org.junit.Assert;
//import org.junit.Test;
//import se.sics.ktoolbox.util.reference.KReference;
//import se.sics.ktoolbox.util.reference.KReferenceException;
//import se.sics.ktoolbox.util.reference.KReferenceFactory;
//
///**
// * @author Alex Ormenisan <aaor@kth.se>
// */
//public class TestCacheKReference {
//
//    @Test
//    public void simpleTest() {
//        KReference<byte[]> ref = KReferenceFactory.getReference(new byte[]{1, 2, 3});
//        Assert.assertTrue(ref.isValid());
//        CacheKReference cRef = CacheKReference.createInstance(ref);
//        Assert.assertTrue(cRef.isValid());
//
//        //release original retain
//        try {
//            ref.release();
//        } catch (KReferenceException ex) {
//            Assert.fail();
//            return;
//        }
//        Assert.assertTrue(ref.isValid());
//        Assert.assertTrue(cRef.isValid());
//
//        //new retain through stream ref
//        Assert.assertTrue(cRef.retain());
//
//        //new release through stream ref
//        try {
//            cRef.release();
//        } catch (KReferenceException ex) {
//            Assert.fail();
//            return;
//        }
//        Assert.assertTrue(ref.isValid());
//        Assert.assertTrue(cRef.isValid());
//
//        //last release through stream ref
//        try {
//            cRef.release();
//        } catch (KReferenceException ex) {
//            Assert.fail();
//            return;
//        }
//        Assert.assertFalse(ref.isValid());
//        Assert.assertFalse(cRef.isValid());
//
//        //retain or release past stream ref validity
//        Assert.assertFalse(cRef.isValid());
//        Assert.assertFalse(cRef.retain());
//        try {
//            cRef.release();
//            Assert.assertTrue(false);
//        } catch (KReferenceException ex) {
//            //expected
//        }
//    }
//
//    @Test
//    public void testKRefStilValid() {
//        KReference<byte[]> ref = KReferenceFactory.getReference(new byte[]{1, 2, 3});
//        Assert.assertTrue(ref.isValid());
//        CacheKReference cRef = CacheKReference.createInstance(ref);
//        Assert.assertTrue(cRef.isValid());
//
//        Assert.assertTrue(ref.isValid());
//        Assert.assertTrue(cRef.isValid());
//
//        //last release through stream ref
//        try {
//            cRef.release();
//        } catch (KReferenceException ex) {
//            Assert.fail();
//            return;
//        }
//        Assert.assertTrue(ref.isValid());
//        Assert.assertFalse(cRef.isValid());
//        Assert.assertTrue(cRef.getValue().isPresent());
//        Assert.assertTrue(cRef.getBaseValue().isPresent());
//
//        //
//        try {
//            ref.release();
//        } catch (KReferenceException ex) {
//            Assert.fail();
//            return;
//        }
//        Assert.assertFalse(ref.isValid());
//        Assert.assertTrue(cRef.getValue().isPresent());
//        Assert.assertFalse(cRef.getBaseValue().isPresent());
//    }
//}
