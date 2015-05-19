import pymssql
import sys
import ProgramResult
import statistics





class Sevkiyat:

    ORDER_RELIABLE_POSITIVE_THRESHOLD = 7
    ORDER_RELIABLE_NEGATIVE_THRESHOLD = -7
    PRODUCTION_PROBLEM_THRESHOLD = 0.5



    def checkIfCustomerExists(self,customerId):
        conn = pymssql.connect(host='127.0.0.1', port='49865', user='sa', password='berna', database='gr2014')
        cur = conn.cursor()
        cur.execute('select count(*) from CARI where ID = %d', customerId)
        row = cur.fetchone()
        count = row[0]
        conn.close()
        if count == 0:
            print "%d 'li musteri veritabaninda bulunamadi" %customerId
            return False
        print "%d 'li musteri veritabaninda bulunmaktadir" % customerId
        return True


    def getSiparisTarihi(self, cariid):
        conn = pymssql.connect(host='127.0.0.1', port='49865', user='sa', password='berna', database='gr2014')
        cur = conn.cursor()
        cur.execute('select sp.TESLIMTAR from SIPARIS s, SIPHAR sp where s.ID = sp.SIPID and s.CARIID=%d', cariid)
        row = cur.fetchone()
        teslimTarihi = 0
        while row:
            teslimTarihi = row[0]
            print "TESLIM TARIHI=%s" % (row[0])
            break
        conn.close()
        return teslimTarihi

    def getOrderIdsForGivenCustomer(self, cariid):
        conn = pymssql.connect(host='127.0.0.1', port='49865', user='sa', password='berna', database='gr2014')
        cur = conn.cursor()
        cur.execute('select distinct s.ID from SIPARIS s, SIPHAR sp where s.ID=sp.SIPID and sp.HADE=0 and s.CARIID=%d', cariid)
        siparisIdler = []
        ids = cur.fetchone()
        while ids:
            siparisIdler.append(ids[0])
            ids = cur.fetchone()
        conn.close()
        if not siparisIdler:
            print "Verilen %d li musterinin siparisi bulunmamaktadir!" %cariid
        else:
            print "%d 'li musterinin %d adet siparisi bulunmaktadir" %(cariid,len(siparisIdler))
            print "Her bir siparis icin Sevk(Tarih) ve Siparis(Teslim Tarihi) arasindaki gun farki ortalamasi hesaplanicaktir."
        return siparisIdler

    def birSiparisinOrtalamasi(self, siparisId):
        conn = pymssql.connect(host='127.0.0.1', port='49865', user='sa', password='berna', database='gr2014')
        cur = conn.cursor()
        cur.execute('select DATEDIFF(day,sip.TESLIMTAR,sevk.TARIH) as GUN_FARKI from SEVK sevk, SIPHAR sip where sip.ID=sevk.SIPHARID and sip.HADE=0 and sip.SIPID=%d', siparisId)
        ids = cur.fetchone()
        ortalamalar = []
        while ids:
            ortalamalar.append(ids[0])
            ids = cur.fetchone()
        conn.close()
        return ortalamalar

    def viyolIleCarpim(self, siparisId, viyolHesaplama):
        conn = pymssql.connect(host='127.0.0.1', port='49865', user='sa', password='berna', database='gr2014')
        cur = conn.cursor()
        cur.execute('select DATEDIFF(day,sip.TESLIMTAR,sevk.TARIH) as GUN_FARKI, sevk.VIYOLADET from SEVK sevk, SIPHAR sip where sip.ID=sevk.SIPHARID and sip.HADE=0 and sip.SIPID=%d', siparisId)
        ids = cur.fetchone()
        while ids:
            viyolHesaplama.append(ids[0]*ids[1])
            ids = cur.fetchone()
        conn.close()
        pass

    def getSiparisTarihiFromId(self, sipid):
        conn = pymssql.connect(host='127.0.0.1', port='49865', user='sa', password='berna', database='gr2014')
        cur = conn.cursor()
        cur.execute('select sp.TESLIMTAR from SIPHAR sp where  sp.SIPID=%d and sp.HADE=0', sipid)
        teslimatTarihleri = cur.fetchall()
        conn.close()
        return teslimatTarihleri

    def getSevkiyatTarihi(self, cariid):
        conn = pymssql.connect(host='127.0.0.1', port='49865', user='sa', password='berna', database='gr2014')
        cur = conn.cursor()
        cur.execute('select sp.TARIH from SEVK sp, SIPARIS s where s.ID = sp.SIPID and s.CARIID=%d', cariid)
        row = cur.fetchone()
        sevkTarihi = 0
        while row:
            sevkTarihi = row[0]
            print "SEVK TARIHI=%s" % (row[0])
            break
        conn.close()
        return sevkTarihi

    def getDayDifference(self,cariid):
        teslimTarihi = self.getSiparisTarihi(cariid)
        sevkTarihi = self.getSevkiyatTarihi(cariid)
        diff = (teslimTarihi - sevkTarihi).days
        return diff

    def uretimKaynaklimi(self,cariid):
        conn = pymssql.connect(host='127.0.0.1', port='49865', user='sa', password='berna', database='gr2014')
        cur = conn.cursor()
        cur.execute('select sp.SEVKTAR, sp.CIKISTAR from LOTS sp where sp.CARIID=%d', cariid)
        row = cur.fetchone()
        sevkTarihi = 0
        while row:
            sevkTarihi = row[0]
            print "SEVKTAR=%s, CIKISTAR=%s " % (row[0], row[1])
            break
        conn.close()
        self.getSiparisTarihi(cariid)
        return sevkTarihi

    def calculateOrderAveragesForGivenIds(self,ids,customerId):
        siparisOrtalamalari = []
        viyolHesaplama = []
        successfulOrderCount = 0
        counter = 0
        ignoredOrderCount=0
        possibleHadeCounter = 0
        for id in ids:
            counter += 1
            siparisinGunFarklari = self.birSiparisinOrtalamasi(id)
            carpimSonucu = self.viyolIleCarpim(id,viyolHesaplama)
            if not siparisinGunFarklari:
                print "%d)%d nolu siparis ile ilgili gun farki bilgisi bulunmamaktadir." %(counter,id)
                possibleHadeCounter += 1
                continue;
            siparisOrtalamasi = sum(siparisinGunFarklari) / float(len(siparisinGunFarklari))
            if siparisOrtalamasi == 0:
                print "%d)%d nolu siparis ortalamasi = 0 o yuzden genel ortalamaya katilmayacaktir. Gun farklari = %s" %(counter,id,str(siparisinGunFarklari))
                ignoredOrderCount += 1
                continue
            else:
                print "%d)%d nolu siparis icindeki turlerin gun farklari su sekildedir: %s ortalama = %f" %(counter,id,str(siparisinGunFarklari), siparisOrtalamasi)
            siparisOrtalamalari.append(siparisOrtalamasi)
            if self.checkIfInReliableInterval(siparisOrtalamasi):
                successfulOrderCount += 1
                print "Siparis ortalamasi istenilen sure icerisinde (%d,%d) gerceklesmistir. Ortalama sure= %f" %(self.ORDER_RELIABLE_NEGATIVE_THRESHOLD, self.ORDER_RELIABLE_POSITIVE_THRESHOLD,siparisOrtalamasi)
            else:
                print "Siparis istenilen surede gerceklesmedi."
        failOrderCount = len(ids)-successfulOrderCount -ignoredOrderCount - possibleHadeCounter
        customerAllOrdersAverage = 0
        if len(siparisOrtalamalari) != 0:
            customerAllOrdersAverage = sum(siparisOrtalamalari) / float(len(siparisOrtalamalari))
            print("Musterinin tum siparisleri icin ortalamasi %f" % customerAllOrdersAverage)
        else:
            print "Hesaplanacak siparis ortalamasi bulunmamaktadir"
        print "%s" %viyolHesaplama
        viyolluOrtalama = statistics.mean(tuple(viyolHesaplama))
        viyolStandartSapma = statistics.stdev(tuple(viyolHesaplama))
        print "Bu musterinin toplam siparis sayisi %d" %len(ids)
        print "Basarili siparis sayisi = %d" %successfulOrderCount
        print "Basarisiz siparis sayisi = %d" %failOrderCount
        print "Ortalamasi 0 oldugu icin hesaba katilmayan siparis sayisi = %d" %ignoredOrderCount
        print "Olasi 'Hade' li siparis sayisi = %d" %possibleHadeCounter
        print "Viyol adeti ve gun carpimi ortalamasi %f ve standart sapmasi = %f" %(viyolluOrtalama,viyolStandartSapma)
        print "Genel siparis ortalamasi= %f" %customerAllOrdersAverage
        if self.checkIfInReliableInterval(customerAllOrdersAverage):
            print "%d 'li musteri guvenilirdir." %customerId
        return customerAllOrdersAverage

    def getFailedOrderIds(self,ids):
        failedOrderIds = []
        for id in ids:
            siparisinGunFarklari = self.birSiparisinOrtalamasi(id)
            if not siparisinGunFarklari:
                continue;
            siparisOrtalamasi = sum(siparisinGunFarklari) / float(len(siparisinGunFarklari))
            if siparisOrtalamasi == 0:
                continue
            if self.checkIfInReliableInterval(siparisOrtalamasi):
               continue
            failedOrderIds.append(id)
        return failedOrderIds

    def checkIfInReliableInterval(self,value):
         if value <= self.ORDER_RELIABLE_POSITIVE_THRESHOLD and value >= self.ORDER_RELIABLE_NEGATIVE_THRESHOLD:
             return True
         return False

    def checkIfRootCauseIsProduction(self,failedOrderIds):
        print "Sorunlu siparis numaralari sunlardir = %s" %tuple(failedOrderIds)
        conn = pymssql.connect(host='127.0.0.1', port='49865', user='sa', password='berna', database='gr2014')
        cur = conn.cursor()
        cur.execute('select DATEDIFF(day,sip.TESLIMTAR,l.CIKISTAR),r.SIPHARID from LOTS l,REZERV r,SIPHAR sip where r.LOTID = l.LOTID and r.SIPHARID = sip.ID and r.SIPID = sip.SIPID and r.SIPID IN (%s)', tuple(failedOrderIds))
        ids = cur.fetchone()
        uretimKaynaklilar = []
        while ids:
            print "%d ID'li siparisin uretin gun farki %d 'dir" %(ids[1],ids[0])
            isReliable = self.checkIfInReliableInterval(ids[0])
            if isReliable:
                ids = cur.fetchone()
                continue
            uretimKaynaklilar.append(ids[1])
            ids = cur.fetchone()
        conn.close()
        print "Uretim kaynakli urun sayisi %d kadardir" %len(uretimKaynaklilar)
        ratio = len(uretimKaynaklilar) / len(failedOrderIds)
        print "Orani %d 'dir" %ratio
        if ratio > self.PRODUCTION_PROBLEM_THRESHOLD:
            return True
        return False

    def isCustomerReliable(self,customerId):
        customerExist = self.checkIfCustomerExists(customerId)
        if customerExist is False:
            return ProgramResult.CUSTOMER_NOT_FOUND
        orderIds = self.getOrderIdsForGivenCustomer(customerId)
        if not orderIds:
            return ProgramResult.NO_RECORD_FOR_CUSTOMER
        customerOrdersAvarage = self.calculateOrderAveragesForGivenIds(orderIds,customerId)
        if self.checkIfInReliableInterval(customerOrdersAvarage):
            return ProgramResult.CUSTOMER_IS_RELIABLE
        failedOrderIds = self.getFailedOrderIds(orderIds)
        if not failedOrderIds :
            raise Exception(" %d FAILED ORDER ID BOS OLMAMALIYDI" %customerId)
        print "Musteri ortalamasi guvenirlik sinirlari disindadir.Sorun uretim ya da musteri kaynakli olabilir. Ilk olarak uretim durumu kontrol edilir."
        isProduction = self.checkIfRootCauseIsProduction(failedOrderIds)
        if isProduction:
            return ProgramResult.CUSTOMER_IS_RELIABLE
        return ProgramResult.CUSTOMER_IS_NOT_RELIABLE

    def getAllCustomerIds(self):
        conn = pymssql.connect(host='127.0.0.1', port='49865', user='sa', password='berna', database='gr2014')
        cur = conn.cursor()
        cur.execute('select DISTINCT ID from CARI')
        row = cur.fetchone()
        customerIDs = []
        while row:
            customerIDs.append(row[0])
            row = cur.fetchone()
        conn.close()
        return customerIDs
    def getResultString(self,result):
        return{
            ProgramResult.CUSTOMER_IS_NOT_RELIABLE : "Musteri guvenilir degil.",
            ProgramResult.CUSTOMER_IS_RELIABLE : "Musteri guvenilir",
            ProgramResult.NO_RECORD_FOR_CUSTOMER : "Musteri hakkinda kayit yok",
            ProgramResult.CUSTOMER_NOT_FOUND : "Bu Musteri kayitli degildir",
        }.get(result)

    def testForAllCustomer(self):
        notReliableCustomers = []
        reliableCustomers = []
        noRecordFound = []
        customerIds = self.getAllCustomerIds()
        for customerId in customerIds:
            result = self.isCustomerReliable(customerId)
            print "customer id = %d" %customerId
            print "result is = %s" %str(self.getResultString(result))
            if result == ProgramResult.CUSTOMER_IS_NOT_RELIABLE:
                notReliableCustomers.append(customerId)
            elif result == ProgramResult.CUSTOMER_IS_RELIABLE:
                reliableCustomers.append(customerId)
            elif result == ProgramResult.NO_RECORD_FOR_CUSTOMER:
                noRecordFound.append(customerId)
        print " Guvenilir = %d , Guvenilir degil = %d , Kayit olmayanlar = %d" %(len(reliableCustomers), len(notReliableCustomers), len(noRecordFound))
        print " Toplam = %d " %len(customerIds)
        print " Islenen Toplam = %d" %(len(reliableCustomers)+ len(notReliableCustomers)+ len(noRecordFound))
        print notReliableCustomers

def main():
        s = Sevkiyat()
        s.isCustomerReliable(1637)
        #s.testForAllCustomer()


if  __name__ =='__main__':main()