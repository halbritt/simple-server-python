from flask import Flask, url_for
app = Flask(__name__)

@app.route('/')
def api_root():
    return 'Welcome'

@app.route('/RESTTest')
def rest_test():
    return '''{
  "IIM.IP21_TextValuesResponse": {
    "IIM.IP21_TextValuesOutput": {
      "IIM.row": [
        {
          "IIM.NAME": "HPPX0053.MachineID",
          "IIM.IP_DESCRIPTION": "Harlow M/C Asset Reference",
          "IIM.IP_INPUT_VALUE": "HPPX0053",
          "IIM.IP_INPUT_TIME": "05-JAN-16 18:07:19.2",
          "IIM.IP_INPUT_QUALITY": "Good",
          "IIM.IP_TAG_TYPE": "Text"
        },
        {
          "IIM.NAME": "HPPX0053.MachineDesc",
          "IIM.IP_DESCRIPTION": "Full Machine Description",
          "IIM.IP_INPUT_VALUE": "Kilian T100 Tablet Press",
          "IIM.IP_INPUT_TIME": "05-JAN-16 18:07:19.2",
          "IIM.IP_INPUT_QUALITY": "Good",
          "IIM.IP_TAG_TYPE": "Text"
        },
        {
          "IIM.NAME": "HPPX0053.OperatorName",
          "IIM.IP_DESCRIPTION": "Operator Name",
          "IIM.IP_INPUT_VALUE": "String 22",
          "IIM.IP_INPUT_TIME": "05-JAN-16 18:07:19.2",
          "IIM.IP_INPUT_QUALITY": "Good",
          "IIM.IP_TAG_TYPE": "Text"
        },
        {
          "IIM.NAME": "HPPX0053.GenBatchNumber",
          "IIM.IP_DESCRIPTION": "IMS Generated Unique Number",
          "IIM.IP_INPUT_VALUE": "NULL",
          "IIM.IP_INPUT_TIME": "05-JAN-16 18:07:19.9",
          "IIM.IP_INPUT_QUALITY": "Good",
          "IIM.IP_TAG_TYPE": "Text"
        },
        {
          "IIM.NAME": "HPPX0053.BatchNumber",
          "IIM.IP_DESCRIPTION": "Batch Number - Unique Batch ID",
          "IIM.IP_INPUT_VALUE": "String 20",
          "IIM.IP_INPUT_TIME": "05-JAN-16 18:07:19.9",
          "IIM.IP_INPUT_QUALITY": "Good",
          "IIM.IP_TAG_TYPE": "Text"
        },
        {
          "IIM.NAME": "HPPX0053.NBNumber",
          "IIM.IP_DESCRIPTION": "Notebook Number",
          "IIM.IP_INPUT_VALUE": "String 21",
          "IIM.IP_INPUT_TIME": "05-JAN-16 18:07:19.9",
          "IIM.IP_INPUT_QUALITY": "Good",
          "IIM.IP_TAG_TYPE": "Text"
        },
        {
          "IIM.NAME": "HPPX0053.SubBatchNum",
          "IIM.IP_DESCRIPTION": "Sub Batch Number",
          "IIM.IP_INPUT_VALUE": "NULL",
          "IIM.IP_INPUT_TIME": "05-JAN-16 18:07:19.2",
          "IIM.IP_INPUT_QUALITY": "Good",
          "IIM.IP_TAG_TYPE": "Text"
        },
        {
          "IIM.NAME": "WHPPX0053.SubBatchNum",
          "IIM.IP_DESCRIPTION": "Sub Batch Number",
          "IIM.IP_INPUT_VALUE": "NULL",
          "IIM.IP_INPUT_TIME": "21-JAN-13 09:58:06.2",
          "IIM.IP_INPUT_QUALITY": "Good",
          "IIM.IP_TAG_TYPE": "Text"
        },
        {
          "IIM.NAME": "HPPX0053.CompoundNum",
          "IIM.IP_DESCRIPTION": "Compound Number",
          "IIM.IP_INPUT_VALUE": "String 26",
          "IIM.IP_INPUT_TIME": "05-JAN-16 18:07:19.2",
          "IIM.IP_INPUT_QUALITY": "Good",
          "IIM.IP_TAG_TYPE": "Text"
        },
        {
          "IIM.NAME": "WHPPX0053.CompoundNum",
          "IIM.IP_DESCRIPTION": "Compound Number",
          "IIM.IP_INPUT_VALUE": "NULL",
          "IIM.IP_INPUT_TIME": "21-JAN-13 09:58:06.2",
          "IIM.IP_INPUT_QUALITY": "Good",
          "IIM.IP_TAG_TYPE": "Text"
        },
        {
          "IIM.NAME": "HPPX0053.SubstanceName",
          "IIM.IP_DESCRIPTION": "Substance Name",
          "IIM.IP_INPUT_VALUE": "String 25",
          "IIM.IP_INPUT_TIME": "05-JAN-16 18:07:19.2",
          "IIM.IP_INPUT_QUALITY": "Good",
          "IIM.IP_TAG_TYPE": "Text"
        },
        {
          "IIM.NAME": "WHPPX0053.SubstanceName",
          "IIM.IP_DESCRIPTION": "Substance Name",
          "IIM.IP_INPUT_VALUE": "676",
          "IIM.IP_INPUT_TIME": "11-JUN-12 16:22:32.7",
          "IIM.IP_INPUT_QUALITY": "Good",
          "IIM.IP_TAG_TYPE": "Text"
        },
        {
          "IIM.NAME": "HPPX0053.ProductCode",
          "IIM.IP_DESCRIPTION": "Product Code",
          "IIM.IP_INPUT_VALUE": "String 27",
          "IIM.IP_INPUT_TIME": "05-JAN-16 18:07:19.2",
          "IIM.IP_INPUT_QUALITY": "Good",
          "IIM.IP_TAG_TYPE": "Text"
        },
        {
          "IIM.NAME": "WHPPX0053.ProductCode",
          "IIM.IP_DESCRIPTION": "Product Code",
          "IIM.IP_INPUT_VALUE": "NULL",
          "IIM.IP_INPUT_TIME": "21-JAN-13 09:58:06.2",
          "IIM.IP_INPUT_QUALITY": "Good",
          "IIM.IP_TAG_TYPE": "Text"
        },
        {
          "IIM.NAME": "HPPX0053.RoomNumber",
          "IIM.IP_DESCRIPTION": "Room Number",
          "IIM.IP_INPUT_VALUE": "String 24",
          "IIM.IP_INPUT_TIME": "05-JAN-16 18:07:19.2",
          "IIM.IP_INPUT_QUALITY": "Good",
          "IIM.IP_TAG_TYPE": "Text"
        },
        {
          "IIM.NAME": "HPPX0053.GnBatchEvntNum",
          "IIM.IP_DESCRIPTION": "Event Number",
          "IIM.IP_INPUT_VALUE": "NULL",
          "IIM.IP_INPUT_TIME": "05-JAN-16 18:07:19.2",
          "IIM.IP_INPUT_QUALITY": "Good",
          "IIM.IP_TAG_TYPE": "Text"
        },
        {
          "IIM.NAME": "HPPX0053.Campaign",
          "IIM.IP_DESCRIPTION": "Campaign",
          "IIM.IP_INPUT_VALUE": "OF00000187",
          "IIM.IP_INPUT_TIME": "05-JAN-16 16:15:12.1",
          "IIM.IP_INPUT_QUALITY": "Bad",
          "IIM.IP_TAG_TYPE": "Text"
        },
        {
          "IIM.NAME": "HPPX0053.ProductName",
          "IIM.IP_DESCRIPTION": "ProductName",
          "IIM.IP_INPUT_VALUE": "IIM123456",
          "IIM.IP_INPUT_TIME": "05-JAN-16 16:15:12.1",
          "IIM.IP_INPUT_QUALITY": "Bad",
          "IIM.IP_TAG_TYPE": "Text"
        },
        {
          "IIM.NAME": "HPPX0053.FeederKey",
          "IIM.IP_DESCRIPTION": "FeederKey",
          "IIM.IP_INPUT_VALUE": "F1C1",
          "IIM.IP_INPUT_TIME": "05-JAN-16 16:15:12.1",
          "IIM.IP_INPUT_QUALITY": "Bad",
          "IIM.IP_TAG_TYPE": "Text"
        },
        {
          "IIM.NAME": "HPPX0053.ProductID_Feede",
          "IIM.IP_DESCRIPTION": "ProductID_Feeder1",
          "IIM.IP_INPUT_VALUE": "NULL",
          "IIM.IP_INPUT_TIME": "05-JAN-16 16:15:12.1",
          "IIM.IP_INPUT_QUALITY": "Bad",
          "IIM.IP_TAG_TYPE": "Text"
        },
        {
          "IIM.NAME": "HPPX0053.ProductID_Feede",
          "IIM.IP_DESCRIPTION": "ProductID_Feeder2",
          "IIM.IP_INPUT_VALUE": "NULL",
          "IIM.IP_INPUT_TIME": "05-JAN-16 16:15:12.1",
          "IIM.IP_INPUT_QUALITY": "Bad",
          "IIM.IP_TAG_TYPE": "Text"
        },
        {
          "IIM.NAME": "HPPX0053.ProductID_Feede",
          "IIM.IP_DESCRIPTION": "ProductID_Feeder3",
          "IIM.IP_INPUT_VALUE": "NULL",
          "IIM.IP_INPUT_TIME": "05-JAN-16 16:15:12.1",
          "IIM.IP_INPUT_QUALITY": "Bad",
          "IIM.IP_TAG_TYPE": "Text"
        },
        {
          "IIM.NAME": "HPPX0053.ProductID_Feede",
          "IIM.IP_DESCRIPTION": "ProductID_Feeder4",
          "IIM.IP_INPUT_VALUE": "NULL",
          "IIM.IP_INPUT_TIME": "05-JAN-16 16:15:12.1",
          "IIM.IP_INPUT_QUALITY": "Bad",
          "IIM.IP_TAG_TYPE": "Text"
        },
        {
          "IIM.NAME": "HPPX0053.Unit",
          "IIM.IP_DESCRIPTION": "Unit",
          "IIM.IP_INPUT_VALUE": "Continuous Dry Blender",
          "IIM.IP_INPUT_TIME": "05-JAN-16 16:15:12.1",
          "IIM.IP_INPUT_QUALITY": "Bad",
          "IIM.IP_TAG_TYPE": "Text"
        },
        {
          "IIM.NAME": "HPPX0053.BlenderKey",
          "IIM.IP_DESCRIPTION": "BlenderKey",
          "IIM.IP_INPUT_VALUE": "F1C1",
          "IIM.IP_INPUT_TIME": "05-JAN-16 16:15:12.1",
          "IIM.IP_INPUT_QUALITY": "Bad",
          "IIM.IP_TAG_TYPE": "Text"
        },
        {
          "IIM.NAME": "HPPX0053.MAN_ROOM-ACC-TS",
          "IIM.IP_DESCRIPTION": "MAN_ROOM-ACC-TS",
          "IIM.IP_INPUT_VALUE": "NULL",
          "IIM.IP_INPUT_TIME": "05-JAN-16 18:07:19.2",
          "IIM.IP_INPUT_QUALITY": "Suspect",
          "IIM.IP_TAG_TYPE": "Text"
        },
        {
          "IIM.NAME": "HPPX0053.MAN_ROOM-Humid-",
          "IIM.IP_DESCRIPTION": "MAN_ROOM-Humid-TS",
          "IIM.IP_INPUT_VALUE": "NULL",
          "IIM.IP_INPUT_TIME": "05-JAN-16 18:07:19.2",
          "IIM.IP_INPUT_QUALITY": "Suspect",
          "IIM.IP_TAG_TYPE": "Text"
        },
        {
          "IIM.NAME": "HPPX0053.MAN_ROOM-INC-TS",
          "IIM.IP_DESCRIPTION": "MAN_ROOM-INC-TSText",
          "IIM.IP_INPUT_VALUE": "NULL",
          "IIM.IP_INPUT_TIME": "05-JAN-16 18:07:19.2",
          "IIM.IP_INPUT_QUALITY": "Suspect",
          "IIM.IP_TAG_TYPE": "Text"
        },
        {
          "IIM.NAME": "HPPX0053.MAN_ROOM-Temp-T",
          "IIM.IP_DESCRIPTION": "MAN_ROOM-Temp-TS",
          "IIM.IP_INPUT_VALUE": "NULL",
          "IIM.IP_INPUT_TIME": "05-JAN-16 18:07:19.2",
          "IIM.IP_INPUT_QUALITY": "Suspect",
          "IIM.IP_TAG_TYPE": "Text"
        },
        {
          "IIM.NAME": "HPPX0053.MAN_ROOM-Dew-T",
          "IIM.IP_DESCRIPTION": "MAN_ROOM-Dew-T",
          "IIM.IP_INPUT_VALUE": "NULL",
          "IIM.IP_INPUT_TIME": "05-JAN-16 18:07:19.2",
          "IIM.IP_INPUT_QUALITY": "Suspect",
          "IIM.IP_TAG_TYPE": "Text"
        },
        {
          "IIM.NAME": "Tandem.CDBatch",
          "IIM.IP_DESCRIPTION": "Tandem.CDBatch",
          "IIM.IP_INPUT_VALUE": "NULL",
          "IIM.IP_INPUT_TIME": "05-JAN-16 18:07:19.9",
          "IIM.IP_INPUT_QUALITY": "Good",
          "IIM.IP_TAG_TYPE": "Text"
        },
        {
          "IIM.NAME": "Tandem.CDOperator",
          "IIM.IP_DESCRIPTION": "Tandem.CDOperator",
          "IIM.IP_INPUT_VALUE": "NULL",
          "IIM.IP_INPUT_TIME": "05-JAN-16 18:07:19.9",
          "IIM.IP_INPUT_QUALITY": "Good",
          "IIM.IP_TAG_TYPE": "Text"
        },
        {
          "IIM.NAME": "Tandem.CDResNIRNameSpecF",
          "IIM.IP_DESCRIPTION": "Tandem.CDResNIRNameSpecFile",
          "IIM.IP_INPUT_VALUE": "NULL",
          "IIM.IP_INPUT_TIME": "05-JAN-16 18:07:19.9",
          "IIM.IP_INPUT_QUALITY": "Good",
          "IIM.IP_TAG_TYPE": "Text"
        }
      ]
    }
  }
}'''

if __name__ == '__main__':
    app.run()