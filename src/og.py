import json
from helper import FileHelper, S3Helper
from trp import Document
import boto3
import re

QUERY_STRINGS = [
    'negro',
    'caucasian',
    'african',
    'race',
    'employed for domestic purposes only'
]

class OutputGenerator:
    def __init__(self, documentId, response, bucketName, objectName, forms, tables, ddb):
        self.documentId = documentId
        self.response = response
        self.bucketName = bucketName
        self.objectName = objectName
        self.forms = False
        self.tables = tables
        self.ddb = ddb
        self.queryStrings = QUERY_STRINGS

        self.outputPath = "{}-analysis/{}/".format(objectName, documentId)

        self.document = Document(self.response)
        self.docText = ''

    def saveItem(self, pk, sk, output):

        jsonItem = {}
        jsonItem['documentId'] = pk
        jsonItem['outputType'] = sk
        jsonItem['outputPath'] = output

        self.ddb.put_item(Item=jsonItem)

    def queryText(self):
        matchTexts = []
        matchTerms = []
        maxLen = len(self.docText) - 1
        docTextNoNewlines = self.docText.replace('\n', ' ')
        searchDocText = self.docText.lower().replace('\n', ' ')

        for string in self.queryStrings:
            matches = re.finditer(string, searchDocText)
            for match in matches:
                if match:
                    matchTexts.append(docTextNoNewlines[max(match.start()-200, 0):min(match.end() + 200, maxLen)])

            if matchTexts:
                matchTerms.append(string)

        self.ddb.put_item(Item={'documentId': self.documentId,
                                'outputType': 'matchOutput',
                                'objectName': self.objectName,
                                'isMatch': bool(matchTexts),
                                'matchedTexts': matchTexts,
                                'matchedTerms': matchTerms
                                })

    def _outputText(self, page, p):
        text = page.text
        opath = "{}page-{}-text.txt".format(self.outputPath, p)
        S3Helper.writeToS3(text, self.bucketName, opath)
        self.saveItem(self.documentId, "page-{}-Text".format(p), opath)

        textInReadingOrder = page.getTextInReadingOrder()
        opath = "{}page-{}-text-inreadingorder.txt".format(self.outputPath, p)
        S3Helper.writeToS3(textInReadingOrder, self.bucketName, opath)
        self.saveItem(self.documentId, "page-{}-TextInReadingOrder".format(p), opath)

    def _outputForm(self, page, p):
        csvData = []
        for field in page.form.fields:
            csvItem  = []
            if(field.key):
                csvItem.append(field.key.text)
            else:
                csvItem.append("")
            if(field.value):
                csvItem.append(field.value.text)
            else:
                csvItem.append("")
            csvData.append(csvItem)
        csvFieldNames = ['Key', 'Value']
        opath = "{}page-{}-forms.csv".format(self.outputPath, p)
        S3Helper.writeCSV(csvFieldNames, csvData, self.bucketName, opath)
        self.saveItem(self.documentId, "page-{}-Forms".format(p), opath)

    def _outputTable(self, page, p):

        csvData = []
        for table in page.tables:
            csvRow = []
            csvRow.append("Table")
            csvData.append(csvRow)
            for row in table.rows:
                csvRow  = []
                for cell in row.cells:
                    csvRow.append(cell.text)
                csvData.append(csvRow)
            csvData.append([])
            csvData.append([])

        opath = "{}page-{}-tables.csv".format(self.outputPath, p)
        S3Helper.writeCSVRaw(csvData, self.bucketName, opath)
        self.saveItem(self.documentId, "page-{}-Tables".format(p), opath)

    def run(self):

        if(not self.document.pages):
            return

        opath = "{}response.json".format(self.outputPath)
        S3Helper.writeToS3(json.dumps(self.response), self.bucketName, opath)
        self.saveItem(self.documentId, 'Response', opath)

        print("Total Pages in Document: {}".format(len(self.document.pages)))

        docText = ""

        p = 1
        for page in self.document.pages:

            opath = "{}page-{}-response.json".format(self.outputPath, p)
            S3Helper.writeToS3(json.dumps(page.blocks), self.bucketName, opath)
            self.saveItem(self.documentId, "page-{}-Response".format(p), opath)

            self._outputText(page, p)

            docText = docText + page.text + "\n"

            if(self.forms):
                self._outputForm(page, p)

            if(self.tables):
                self._outputTable(page, p)

            p = p + 1

        # Output full document text to a separate file.
        self.docText = docText
        opath = "{}fullText.txt".format(self.outputPath)
        S3Helper.writeToS3(docText, self.bucketName, opath)
        self.saveItem(self.documentId, "{}fullText".format(self.outputPath), opath)
        self.queryText()
