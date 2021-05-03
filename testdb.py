import boto3
import json
import uuid
import datetime
from decimal import Decimal


def get_context_for_url(url):
  sample = {
    "content": {
      "kind": "curriculum.content.video",
      "url": "https://www.udemy.com/02e9db8a-07af-4b30-b4ec-99afa07e32b0",
      "status": "playing",
      "duration": 47.2,
      "position": 12.1,
      "speed": 1
    }
  }
  return sample

def create_recommendations(data):
  context = get_context_for_url('')

  rec = {
    "id": data["id"],
    "sessionId": data["sessionId"],
    "userId": data["userId"],
    "ts": datetime.datetime.now().isoformat(),
    "status": 200,
    "context": context,
    "recommendations": [
      {
        "id": "6120894d-0ce5-4738-adcb-6736b943b202",
        "rank": 1,
        "category": "understanding",
        "action": "video.slower",
        "confidence": 0.53,
        "ts": "2021-04-25T19:15:39.301251"
      },
      {
        "id": "3468367d-0ce5-4738-adcb-6736b943b201",
        "rank": 2,
        "category": "engagement",
        "action": "game.trivia",
        "confidence": 0.48,
        "ts": "2021-04-25T19:15:39.301251"
      },
      {
        "id": "6120894d-0ce5-4738-adcb-6736b943b202",
        "rank": 3,
        "category": "understanding",
        "action": "feedback.ask",
        "confidence": 0.34,
        "ts": "2021-04-25T19:15:39.301251"
      },
      {
        "id": "3468367d-0ce5-4738-adcb-6736b943b201",
        "rank": 4,
        "category": "understanding",
        "action": "video.push",
        "context": {
          "contentId": "7a7d4908-3a33-41f3-909c-c6693b11ee63",
          "kind": "curriculum.content.video",
          "title": "C# Arrays vs. LUA Tables",
          "duration": 157,
          "prompt": True,
          "trigger": {
            "kind": "video.playback",
            "position": 108
          },
          "objective": "comparison_learning",
          "url": "https://www.udemy.com/720fdb8a-07af-4b30-b4ec-99afa07e32b0"
        },
        "confidence": 0.31,
        "ts": "2021-04-25T19:15:39.301251"
      }
    ]
  }
  return rec

dynamodb = boto3.resource('dynamodb', 'us-east-2')
table = dynamodb.Table('mindstreams-user-recommendations')

sample_kinesis_event = '{"id":"3e549a40-8efe-4da2-a695-d0872c87e8d2","sessionId":"a3ce729a-d103-495f-a9ee-6adcf0512540","userId":"scottbeaudreau@hotmail.com","deviceId":"insight-f5bacb9d","componentId":"f5bacb9d-502b-461a-9478-105770bbeb3c","ts":"2021-05-02T12:24:58.719375","metadata":{"appId":"com.chloebeaudreau.mindstreams","headset":{"connectedBy":"dongle","dongle":"6ff","firmware":"931","isDfuMode":false,"isVirtual":true,"settings":{"eegRate":128,"eegRes":14,"memsRate":64,"memsRes":14,"mode":"INSIGHT"},"status":"connected"},"started":"2021-05-02T12:24:41.800-05:00","status":"activated"},"eeg":[],"cognitive":[{"engagement":0.493726,"excitement":0.60956,"excitementLast1Min":0,"stress":0.639118,"relaxation":0.55468,"interest":0.659083,"focus":0.560039,"ts":1619672803.8549}],"facial":[{"eyes":"neutral","upperFace":"frown","upperFacePower":0.5,"lowerFace":"clench","lowerFacePower":0.5,"ts":1619672803.3237}],"motion":[{"n":35,"q0":0.679822,"q1":0.203125,"q2":0.703125,"q3":-0.046875,"ax":1.000015,"ay":-0.156252,"az":0.062501,"mx":-12.495691,"my":-38.281834,"mz":76.563667,"ts":1619672803.4096},{"n":40,"q0":0.679822,"q1":0.203125,"q2":0.703125,"q3":-0.046875,"ax":1.000015,"ay":-0.187503,"az":0.062501,"mx":-12.680998,"my":-38.281834,"mz":76.563667,"ts":1619672803.4877},{"n":45,"q0":0.679822,"q1":0.203125,"q2":0.703125,"q3":-0.046875,"ax":1.000015,"ay":-0.187503,"az":0.062501,"mx":-20.063967,"my":-38.281834,"mz":76.563667,"ts":1619672803.5659},{"n":50,"q0":0.679822,"q1":0.203125,"q2":0.703125,"q3":-0.046875,"ax":1.000015,"ay":-0.187503,"az":0.062501,"mx":-11.847572,"my":-38.281834,"mz":76.563667,"ts":1619672803.644},{"n":55,"q0":0.679822,"q1":0.203125,"q2":0.703125,"q3":-0.046875,"ax":1.000015,"ay":-0.187503,"az":0.093751,"mx":-6.995873,"my":-38.281834,"mz":76.563667,"ts":1619672803.7221},{"n":60,"q0":0.679822,"q1":0.203125,"q2":0.703125,"q3":-0.046875,"ax":1.000015,"ay":-0.187503,"az":0.062501,"mx":-10.367104,"my":-38.281834,"mz":76.563667,"ts":1619672803.8002},{"n":0,"q0":0.679822,"q1":0.203125,"q2":0.703125,"q3":-0.046875,"ax":1.000015,"ay":-0.156252,"az":0.062501,"mx":-16.866152,"my":-38.281834,"mz":76.563667,"ts":1619672803.8627},{"n":5,"q0":0.679822,"q1":0.203125,"q2":0.703125,"q3":-0.046875,"ax":1.000015,"ay":-0.187503,"az":0.062501,"mx":-13.787477,"my":-38.281834,"mz":76.563667,"ts":1619672803.9409},{"n":10,"q0":0.679822,"q1":0.203125,"q2":0.703125,"q3":-0.046875,"ax":1.031266,"ay":-0.156252,"az":0.062501,"mx":-11.969551,"my":-38.281834,"mz":76.563667,"ts":1619672804.019},{"n":15,"q0":0.679822,"q1":0.203125,"q2":0.703125,"q3":-0.046875,"ax":1.000015,"ay":-0.187503,"az":0.062501,"mx":-16.125765,"my":-38.281834,"mz":76.563667,"ts":1619672804.0971},{"n":20,"q0":0.679822,"q1":0.203125,"q2":0.703125,"q3":-0.046875,"ax":1.000015,"ay":-0.187503,"az":0.062501,"mx":-9.522102,"my":-38.281834,"mz":76.563667,"ts":1619672804.1752},{"n":25,"q0":0.679822,"q1":0.203125,"q2":0.703125,"q3":-0.046875,"ax":1.000015,"ay":-0.156252,"az":0.062501,"mx":-5.622706,"my":-38.281834,"mz":76.563667,"ts":1619672804.2534},{"n":30,"q0":0.679822,"q1":0.203125,"q2":0.703125,"q3":-0.046875,"ax":1.031266,"ay":-0.156252,"az":0.062501,"mx":-3.320152,"my":-38.281834,"mz":76.563667,"ts":1619672804.3315},{"n":35,"q0":0.679822,"q1":0.203125,"q2":0.703125,"q3":-0.046875,"ax":1.000015,"ay":-0.187503,"az":0.062501,"mx":-8.579446,"my":-38.281834,"mz":76.563667,"ts":1619672804.4096}]}'

obj = json.loads(sample_kinesis_event, parse_float=Decimal)

item = create_recommendations(obj)
item["context"]["content"]["duration"] = Decimal(str(item["context"]["content"]["duration"]))
item["context"]["content"]["position"] = Decimal(str(item["context"]["content"]["position"]))
item["context"]["content"]["speed"] = Decimal(str(item["context"]["content"]["speed"]))
recs = item["recommendations"]
for i in range(len(recs)):
  r = recs[i]
  r["confidence"] = Decimal(str(r["confidence"]))

print(item)
table.put_item(Item = item)