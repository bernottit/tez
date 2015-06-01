import pymssql
import sys
import ProgramResult
import statistics

class Sevkiyat:
    def __init__(self,log = False):
        self.log = log
        self.conn = pymssql.connect(host='127.0.0.1', port='49865', user='sa', password='berna', database='gr2014')
        self.cur = self.conn.cursor()

    ORDER_RELIABLE_POSITIVE_THRESHOLD = 7
    ORDER_RELIABLE_NEGATIVE_THRESHOLD = -7

    def avarageOfOneOrder(self, siparisId):
        self.cur.execute('select DATEDIFF(day,sip.TESLIMTAR,sevk.TARIH) as GUN_FARKI from SEVK sevk, SIPHAR sip where sip.ID=sevk.SIPHARID and sip.HADE=0 and sip.SIPID=%d', siparisId)
        ids = self.cur.fetchone()
        avarages = []
        while ids:
            avarages.append(ids[0])
            ids = self.cur.fetchone()
        return avarages

    def multiplicationWithViol(self, siparisId, calculateViol):
        self.cur.execute('select DATEDIFF(day,sip.TESLIMTAR,sevk.TARIH) as GUN_FARKI, sevk.VIYOLADET from SEVK sevk, SIPHAR sip where sip.ID=sevk.SIPHARID and sip.HADE=0 and sip.SIPID=%d', siparisId)
        ids = self.cur.fetchone()
        while ids:
            calculateViol.append(ids[0]*ids[1])
            ids = self.cur.fetchone()
        pass

    def calculateOrderAveragesForGivenIds(self,ids,customerId):
        avaragesOfOrder = []
        calculateViol = []
        successfulOrderCount = 0
        counter = 0
        ignoredOrderCount=0
        possibleHadeCounter = 0
        for id in ids:
            counter += 1
            dateDifferenceForOrder = self.avarageOfOneOrder(id)
            multiplicationResult = self.multiplicationWithViol(id,calculateViol)
            if not dateDifferenceForOrder:
                if self.log:
                    print "%d)%d nolu siparis ile ilgili gun farki bilgisi bulunmamaktadir." %(counter,id)
                possibleHadeCounter += 1
                continue;
            avarageOrder = sum(dateDifferenceForOrder) / float(len(dateDifferenceForOrder))
            if avarageOrder == 0:
                if self.log:
                    print "%d)%d nolu siparis ortalamasi = 0 o yuzden genel ortalamaya katilmayacaktir. Gun farklari = %s" %(counter,id,str(dateDifferenceForOrder))
                ignoredOrderCount += 1
                continue
            else:
                if self.log:
                    print "%d)%d nolu siparis icindeki turlerin gun farklari su sekildedir: %s ortalama = %f" %(counter,id,str(dateDifferenceForOrder), avarageOrder)
            avaragesOfOrder.append(avarageOrder)
            if self.checkIfInReliableInterval(avarageOrder):
                successfulOrderCount += 1
                if self.log:
                    print "Siparis ortalamasi istenilen sure icerisinde (%d,%d) gerceklesmistir. Ortalama sure= %f" %(self.ORDER_RELIABLE_NEGATIVE_THRESHOLD, self.ORDER_RELIABLE_POSITIVE_THRESHOLD,avarageOrder)
            else:
                if self.log:
                    print "Siparis istenilen surede gerceklesmedi."
        failOrderCount = len(ids)-successfulOrderCount -ignoredOrderCount - possibleHadeCounter
        customerAllOrdersAverage = 0
        if len(avaragesOfOrder) != 0:
            customerAllOrdersAverage = sum(avaragesOfOrder) / float(len(avaragesOfOrder))
            if self.log:
                print("Musterinin tum siparisleri icin ortalamasi %f" % customerAllOrdersAverage)
        else:
            if self.log:
                print "Hesaplanacak siparis ortalamasi bulunmamaktadir"
        if self.log:
            print "%s" %calculateViol
        standartDeviationOfViol = 0
        avaragesWithViol = 0
        if len(calculateViol)<1:
            if self.log:
                print "Hic bir siparisi olmadigi icin ortalama hesaplanamaz."
        else:
            avaragesWithViol = statistics.mean(tuple(calculateViol))
        if len(calculateViol)<2:
            if self.log:
                print "Tek bir siparisi oldugu icin standart sapma hesaplanamaz."
        else:
            standartDeviationOfViol = statistics.stdev(tuple(calculateViol))
        if self.log:
            print "Bu musterinin toplam siparis sayisi %d" %len(ids)
            print "Basarili siparis sayisi = %d" %successfulOrderCount
            print "Basarisiz siparis sayisi = %d" %failOrderCount
            print "Ortalamasi 0 oldugu icin hesaba katilmayan siparis sayisi = %d" %ignoredOrderCount
            print "Olasi 'Hade' li siparis sayisi = %d" %possibleHadeCounter
            print "Viyol adeti ve gun carpimi ortalamasi %f ve standart sapmasi = %f" %(avaragesWithViol,standartDeviationOfViol)
            print "Genel siparis ortalamasi= %f" %customerAllOrdersAverage
        if self.checkIfInReliableInterval(customerAllOrdersAverage):
            if self.log:
                print "%d 'li musteri guvenilirdir." %customerId
        return customerAllOrdersAverage

    def getFailedOrderIds(self,ids):
        failedOrderIds = []
        for id in ids:
            theDifferenceOfOrders = self.avarageOfOneOrder(id)
            if not theDifferenceOfOrders:
                continue;
            avaragesOrder = sum(theDifferenceOfOrders) / float(len(theDifferenceOfOrders))
            if avaragesOrder == 0:
                continue
            if self.checkIfInReliableInterval(avaragesOrder):
               continue
            failedOrderIds.append(id)
        return failedOrderIds

    def checkIfInReliableInterval(self,value):
         if value <= self.ORDER_RELIABLE_POSITIVE_THRESHOLD and value >= self.ORDER_RELIABLE_NEGATIVE_THRESHOLD:
             return True
         return False

    def checkIfRootCauseIsProduction(self,failedOrderIds):
        if self.log:
            print "Failed order id sayisi %d"%len(failedOrderIds)
            print "Sorunlu siparis numaralari sunlardir = %s" %failedOrderIds
        self.cur.execute('select DATEDIFF(day,sip.TESLIMTAR,l.CIKISTAR),r.SIPHARID from LOTS l,REZERV r,SIPHAR sip where r.LOTID = l.LOTID and r.SIPHARID = sip.ID and r.SIPID = sip.SIPID and r.SIPID IN (%s)', tuple(failedOrderIds))
        ids = self.cur.fetchone()
        rootCauseIsProduction = []
        totalItems = 0
        while ids:
            totalItems += 1
            if self.log:
                print "%d ID'li siparisin uretim gun farki %d 'dir" %(ids[1],ids[0])
            isReliable = self.checkIfInReliableInterval(ids[0])
            if isReliable:
                ids = self.cur.fetchone()
                continue
            rootCauseIsProduction.append(ids[1])
            ids = self.cur.fetchone()
        if self.log:
            print "Uretim kaynakli urun sayisi %d kadardir" %len(rootCauseIsProduction)
        ratio = len(rootCauseIsProduction) / float(totalItems)
        if self.log:
            print "Basarisiz siparislerin uretim kaynakli olanlarinin yuzdesi = %f" %ratio
        return ratio

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

    def percentage(self,part,whole):
        return 100*float(part) / float(whole)

    def getResultString(self,result):
        return{
            ProgramResult.CUSTOMER_IS_NOT_RELIABLE : "Musteri guvenilir degil.",
            ProgramResult.CUSTOMER_IS_RELIABLE : "Musteri guvenilir",
            ProgramResult.NO_RECORD_FOR_CUSTOMER : "Musteri hakkinda kayit yok",
            ProgramResult.CUSTOMER_NOT_FOUND : "Bu Musteri kayitli degildir",
        }.get(result)

    def getCustomerTrustPercentage(self,customerId):
        finalPercentage = float(0)
        orderIds = self.getOrderIdsForGivenCustomer(customerId)
        customerOrdersAvarage = self.calculateOrderAveragesForGivenIds(orderIds,customerId)
        if self.checkIfInReliableInterval(customerOrdersAvarage):
            return float(100)
        failedOrderIds = self.getFailedOrderIds(orderIds)
        finalPercentage += self.percentage(len(orderIds)-len(failedOrderIds), len(orderIds))
        productionPercentage = 100-finalPercentage
        if not failedOrderIds :
            raise Exception(" %d FAILED ORDER ID BOS OLMAMALIYDI" %customerId)
        if self.log:
            print "Musteri ortalamasi guvenirlik sinirlari disindadir.Sorun uretim ya da musteri kaynakli olabilir. Ilk olarak uretim durumu kontrol edilir."
        productionRatio = self.checkIfRootCauseIsProduction(failedOrderIds)
        productionPercentage *= productionRatio
        finalPercentage += productionPercentage
        return finalPercentage

    def getCustomerTrustPercentageForTransportation(self,customerId):
        percentages = []
        for dayDifference in range(2,7):
            self.ORDER_RELIABLE_POSITIVE_THRESHOLD = dayDifference
            self.ORDER_RELIABLE_NEGATIVE_THRESHOLD = -dayDifference
            if self.log:
                print"Positive Threshold = %d"%self.ORDER_RELIABLE_POSITIVE_THRESHOLD
                print"Negative Threshold = %d"%self.ORDER_RELIABLE_NEGATIVE_THRESHOLD
            percentage = self.getCustomerTrustPercentage(customerId)
            if self.log:
                print"Percentage %f"%percentage
            percentages.append(percentage)
        percentageMean = statistics.mean(percentages)
        self.conn.close()
        return percentageMean

def main():
        s = Sevkiyat(True)
        percentage = s.getCustomerTrustPercentageForTransportation(2461)
        print"Sonuc %f"%percentage

if  __name__ =='__main__':main()