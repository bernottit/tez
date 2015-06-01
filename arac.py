import pymssql
import sys
import ProgramResult
import statistics


class Arac:
    def __init__(self,log = False):
        self.log = log
        self.conn = pymssql.connect(host='127.0.0.1', port='49865', user='sa', password='berna', database='gr2014')
        self.cur = self.conn.cursor()
    def getFisnoForCustomer(self,customerId):
        self.cur.execute('select DISTINCT sevk.FISNO from SIPARIS sip, SEVK sevk where sevk.SIPID = sip.ID and sip.CARIID = %d', customerId)
        row = self.cur.fetchone()
        aracID = []
        while row:
            aracID.append(row[0])
            row = self.cur.fetchone()
        #conn.close()
        return aracID

    def partialOrderNumberInArac(self,aracNo):
        #conn = pymssql.connect(host='127.0.0.1', port='49865', user='sa', password='berna', database='gr2014')
        #cur = conn.cursor()
        self.cur.execute('select count(*) from SEVK where FISNO = %d', aracNo)
        row = self.cur.fetchone()
        #conn.close()
        partialOrderNumber = row[0]
        return partialOrderNumber

    def customerPartialOrderNumberInArac(self,customerId,aracNo):
        #conn = pymssql.connect(host='127.0.0.1', port='49865', user='sa', password='berna', database='gr2014')
        #cur = conn.cursor()
        self.cur.execute('select count(*)from SIPARIS sip, SEVK sevk where sevk.SIPID = sip.ID and sip.CARIID = %d and sevk.FISNO = %d' % (customerId,aracNo))
        row = self.cur.fetchone()
        #conn.close()
        aracID = row[0]
        return aracID

    def percentage(self,part,whole):
        return 100*float(part) / float(whole)

    def getCustomerTrustPercentageForArac(self,customerId):
        vehicleIds = self.getFisnoForCustomer(customerId)
        if self.log:
            print " %d ID'li musterinin siparislerini tasiyan arac IDleri = %s" %(customerId,vehicleIds)
        percentages = []
        for vehicleId in vehicleIds:
            parcaliSiparisSayisi = self.partialOrderNumberInArac(vehicleId)
            musteri = self.customerPartialOrderNumberInArac(customerId,vehicleId)
            if self.log:
                print "%d nolu aractaki parcali siparis sayisi = %d" %(vehicleId,parcaliSiparisSayisi)
                print "%d 'li musterinin %d nolu aractaki parcali siparis sayisi = %d" %(customerId,vehicleId,musteri)
            percentages.append(self.percentage(musteri,parcaliSiparisSayisi))
        customerRatio = 0
        if len(percentages)<1:
            if self.log:
                print "Musterinin parcali siparis % yoktur."
        else:
            customerRatio = statistics.mean(percentages)
        if self.log:
            print "%d idli musterinin orani %f"%(customerId,customerRatio)
        self.conn.close()
        return customerRatio

    def customerDistribution(self,aracId):
        #conn = pymssql.connect(host='127.0.0.1', port='49865', user='sa', password='berna', database='gr2014')
        #cur = conn.cursor()
        self.cur.execute('select count(*), sip.CARIID from SEVK sevk, SIPARIS sip where sevk.SIPID = sip.ID and sevk.FISNO = %d group by sip.CARIID', aracId)
        row = self.cur.fetchone()
        data = []
        while row:
            partialOrder = row[0]
            musteriId = row[1]
            data.append(partialOrder)
            if self.log:
                print "%d id'li musterinin aractaki parcali siparis sayisi = %d" %(musteriId, partialOrder)
            row = self.cur.fetchone()
        #conn.close()
        if self.log:
            print "%d idli Arac Istatistigi"%aracId
            print "Minimum parcali siparis sayisi = %d " %min(data)
            print "Maximum parcali siparis sayisi = %d " %max(data)
            print "Parcali siparis ortlamasi = %f" %statistics.mean(data)
            print "Parcali siparis standart sapmasi = %f" %statistics.stdev(data)
        return data







def main():
    a = Arac(True)
    customerId =1652
    a.getCustomerTrustPercentageForArac(customerId)


if  __name__ =='__main__':main()