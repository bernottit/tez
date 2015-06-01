__author__ = 'bernottit'

import pymssql
import sys
import ProgramResult
import statistics
from sevkiyat import Sevkiyat
from arac import Arac
from teslimat import Teslimat

class Tez:
    def __init__(self,log = False):
        self.log = log
        self.conn = pymssql.connect(host='127.0.0.1', port='49865', user='sa', password='berna', database='gr2014')
        self.cur = self.conn.cursor()
    def checkIfCustomerExists(self,customerId):
        self.cur.execute('select count(*) from CARI where ID = %d', customerId)
        row = self.cur.fetchone()
        count = row[0]
        if count == 0:
            if self.log:
                print "%d 'li musteri veritabaninda bulunamadi" %customerId
            return False
        if self.log:
            print "%d 'li musteri veritabaninda bulunmaktadir" % customerId
        return True

    def getOrderIdsForGivenCustomer(self, cariid):
        self.cur.execute('select distinct s.ID from SIPARIS s, SIPHAR sp where s.ID=sp.SIPID and sp.HADE=0 and s.CARIID=%d', cariid)
        orderIds = []
        ids = self.cur.fetchone()
        while ids:
            orderIds.append(ids[0])
            ids = self.cur.fetchone()
        if not orderIds:
            if self.log:
                print "Verilen %d li musterinin siparisi bulunmamaktadir!" %cariid
        else:
            if self.log:
                print "%d 'li musterinin %d adet siparisi bulunmaktadir" %(cariid,len(orderIds))
                print "Her bir siparis icin Sevk(Tarih) ve Siparis(Teslim Tarihi) arasindaki gun farki ortalamasi hesaplanicaktir."
        return orderIds

    def getAllCustomerIds(self):
        self.cur.execute('select DISTINCT ID from CARI')
        row = self.cur.fetchone()
        customerIDs = []
        while row:
            customerIDs.append(row[0])
            row = self.cur.fetchone()
        return customerIDs

    def getCustomerName(self,customerId):
        self.cur.execute('select CARIADI from SIPARIS where CARIID=%d',customerId)
        row = self.cur.fetchone()
        name = row[0]
        return name

    def testForAllCustomer(self):
        notReliableCustomers = []
        reliableCustomers = []
        noRecordFound = []
        customerIds = self.getAllCustomerIds()
        for customerId in customerIds:
            result = self.isCustomerReliable(customerId)
            if self.log:
                print "customer id = %d" %customerId
                print "result is = %s" %str(self.getResultString(result))
            if result == ProgramResult.CUSTOMER_IS_NOT_RELIABLE:
                notReliableCustomers.append(customerId)
            elif result == ProgramResult.CUSTOMER_IS_RELIABLE:
                reliableCustomers.append(customerId)
            elif result == ProgramResult.NO_RECORD_FOR_CUSTOMER:
                noRecordFound.append(customerId)
        if self.log:
            print " Guvenilir = %d , Guvenilir degil = %d , Kayit olmayanlar = %d" %(len(reliableCustomers), len(notReliableCustomers), len(noRecordFound))
            print " Toplam = %d " %len(customerIds)
            print " Islenen Toplam = %d" %(len(reliableCustomers)+ len(notReliableCustomers)+ len(noRecordFound))
        print notReliableCustomers

    def getOverallCustomerTrustPercentage(self,customerId):
        percentages = []
        customerExist = self.checkIfCustomerExists(customerId)
        if customerExist is False:
            return ProgramResult.CUSTOMER_NOT_FOUND
        orderIds = self.getOrderIdsForGivenCustomer(customerId)
        if not orderIds:
            return ProgramResult.NO_RECORD_FOR_CUSTOMER
        sevkiyat = Sevkiyat()
        arac = Arac()
        teslimat = Teslimat()
        transportationPercentage =  sevkiyat.getCustomerTrustPercentageForTransportation(customerId)
        partedPercantage = arac.getCustomerTrustPercentageForArac(customerId)
        shippingPercantage = teslimat.customerReliablePercentageAccordingToDeliveryType(customerId)
        percentages.append(transportationPercentage)
        percentages.append(shippingPercantage)
        percentages.append(partedPercantage)
        generalmean= statistics.mean(percentages)
        if self.log:
            print "%d ID'li musterinin sevkiyat guvenirlik yuzdesi = %f" %(customerId,transportationPercentage)
            print "%d ID'li musterinin arac guvenirlik yuzdesi = %f" %(customerId,partedPercantage)
            print "%d ID'li musterinin teslimat guvenirlik yuzdesi = %f" %(customerId,shippingPercantage)
            print "%d ID'li musterinin genel guvenirlik yuzdesi = %f" %(customerId,generalmean)
        if generalmean > 100:
            raise Exception("100den buyuk ortalama olamaz! Customer ID = %d Ortalama = %f" %(customerId,generalmean))
        return generalmean

if __name__ == '__main__':
    tez = Tez(False)
    fo = open("result.txt", "w+")
    ### Tum musteriler icin
    customerIds = tez.getAllCustomerIds()
    count = 0
    print"Total customer number %d"%len(customerIds)
    olmayanMusteriler = 0
    for customerId in customerIds:
        count += 1
        #if count % 100 == 0:
        #    continue
        customerPercentage = tez.getOverallCustomerTrustPercentage(customerId)
        if customerPercentage == ProgramResult.CUSTOMER_NOT_FOUND or customerPercentage == ProgramResult.NO_RECORD_FOR_CUSTOMER:
            x = "%d id'li musteri bulunamadi ya da kaydi yok"%customerId
            fo.write(x)
            fo.write('\n')
            olmayanMusteriler += 1
            continue
        customerName = tez.getCustomerName(customerId)
        a = "%s = %f"%(customerName.strip(),customerPercentage)
        print a
        fo.write(a.encode('utf-8'))
        fo.write('\n')
    fo.close()
    tez.conn.close()
    print "Program Tamamlandi."
    print "Olmayan musteri sayisi = %d"%olmayanMusteriler
    ### Tek musteri icin
    #customerId = 1707
    #tez.getOverallCustomerTrustPercentage(customerId)



