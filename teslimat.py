__author__ = 'bernottit'

import pymssql
import sys
import ProgramResult
import statistics


class Teslimat:
    def __init__(self,log = False):
        self.log = log
        self.conn = pymssql.connect(host='127.0.0.1', port='49865', user='sa', password='berna', database='gr2014')
        self.cur = self.conn.cursor()
    def getOrderIdsForCustomer(self, cariid):
        self.cur.execute('select distinct s.ID from SIPARIS s, SIPHAR sp where s.ID=sp.SIPID and sp.HADE=0 and s.CARIID=%d', cariid)
        siparisIdler = []
        ids = self.cur.fetchone()
        while ids:
            siparisIdler.append(ids[0])
            ids = self.cur.fetchone()
        if not siparisIdler:
            if self.log:
                print "Verilen %d li musterinin siparisi bulunmamaktadir!" %cariid
        else:
            if self.log:
                print "%d 'li musterinin %d adet siparisi bulunmaktadir" %(cariid,len(siparisIdler))
        return siparisIdler

    def getPercentageOfAracForGivenOrderId(self,orderId):
        self.cur.execute('select count(*), SUBSTRING(ps.ARAC,1,2) from sevk s, psevk ps where ps.FISNO = s.FISNO and s.SIPID= %d group by ps.ARAC;', orderId)
        row = self.cur.fetchone()
        aracTypeCount = {}
        while row:
            aracTypeCount[row[1]]=row[0]
            row = self.cur.fetchone()
        return aracTypeCount

    def getCustomerPercentageForAracTypes(self,customerId):
        totalCount={}
        orderIds = self.getOrderIdsForCustomer(customerId)
        for orderId in orderIds:
            birsiparisinkargodagilimi = self.getPercentageOfAracForGivenOrderId(orderId)
            if not bool(birsiparisinkargodagilimi):
                continue
            for tip in birsiparisinkargodagilimi:
                if totalCount.has_key(tip):
                    Count = totalCount[tip]
                    totalCount[tip]+= birsiparisinkargodagilimi[tip]
                else:
                    totalCount[tip] = birsiparisinkargodagilimi[tip]
        if self.log:
            print(totalCount)
        return totalCount
    def customerReliablePercentageAccordingToDeliveryType(self,customerId):
        Total =  self.getCustomerPercentageForAracTypes(customerId)
        percentage = float(100)
        if bool(Total):
            disCount = float(0)
            if Total.has_key("DI"):
                disCount = Total["DI"]
            elif Total.has_key("DA"):
                disCount += Total["DA"]
            else:
                if self.log:
                    print "%d idli musterinin hic dis gonderimi bulunnmuyor!"%customerId
                return percentage
            sumCount = sum(Total.values())
            percentage = float(100) - self.percentage(disCount,sumCount)
            if self.log:
                print "%d idli musteri guvenilirlik yuzdesi = %f" %(customerId,percentage)
        else:
            if self.log:
                print"%d idli musterinin kargo dagilimi bulunmamaktadir"%customerId
        self.conn.close()
        return percentage

    def percentage(self,part,whole):
        return 100*float(part) / float(whole)

def main():
    t = Teslimat(True)
    customerId =1635
    t.customerReliablePercentageAccordingToDeliveryType(customerId)

if  __name__ =='__main__':main()