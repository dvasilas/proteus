# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: s3client.proto

import sys
_b=sys.version_info[0]<3 and (lambda x:x) or (lambda x:x.encode('latin1'))
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor.FileDescriptor(
  name='s3client.proto',
  package='s3client',
  syntax='proto3',
  serialized_options=_b('Z/github.com/dvasilas/proteus/src/protos/s3client'),
  serialized_pb=_b('\n\x0es3client.proto\x12\x08s3client\"*\n\x13\x43reateBucketRequest\x12\x13\n\x0b\x62ucket_name\x18\x01 \x01(\t\"\xbc\x01\n\x10PutObjectRequest\x12\x13\n\x0bobject_name\x18\x01 \x01(\t\x12\x13\n\x0b\x62ucket_name\x18\x02 \x01(\t\x12<\n\nx_amz_meta\x18\x03 \x03(\x0b\x32(.s3client.PutObjectRequest.XAmzMetaEntry\x12\x0f\n\x07\x63ontent\x18\x04 \x01(\t\x1a/\n\rXAmzMetaEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\t:\x02\x38\x01\"\xad\x01\n\x11UpdateTagsRequest\x12\x13\n\x0bobject_name\x18\x01 \x01(\t\x12\x13\n\x0b\x62ucket_name\x18\x02 \x01(\t\x12=\n\nx_amz_meta\x18\x03 \x03(\x0b\x32).s3client.UpdateTagsRequest.XAmzMetaEntry\x1a/\n\rXAmzMetaEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\t:\x02\x38\x01\"\x16\n\x05Reply\x12\r\n\x05reply\x18\x01 \x01(\x05\x32\xc0\x01\n\x02S3\x12@\n\x0c\x43reateBucket\x12\x1d.s3client.CreateBucketRequest\x1a\x0f.s3client.Reply\"\x00\x12:\n\tPutObject\x12\x1a.s3client.PutObjectRequest\x1a\x0f.s3client.Reply\"\x00\x12<\n\nUpdateTags\x12\x1b.s3client.UpdateTagsRequest\x1a\x0f.s3client.Reply\"\x00\x42\x31Z/github.com/dvasilas/proteus/src/protos/s3clientb\x06proto3')
)




_CREATEBUCKETREQUEST = _descriptor.Descriptor(
  name='CreateBucketRequest',
  full_name='s3client.CreateBucketRequest',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='bucket_name', full_name='s3client.CreateBucketRequest.bucket_name', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=28,
  serialized_end=70,
)


_PUTOBJECTREQUEST_XAMZMETAENTRY = _descriptor.Descriptor(
  name='XAmzMetaEntry',
  full_name='s3client.PutObjectRequest.XAmzMetaEntry',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='key', full_name='s3client.PutObjectRequest.XAmzMetaEntry.key', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='value', full_name='s3client.PutObjectRequest.XAmzMetaEntry.value', index=1,
      number=2, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=_b('8\001'),
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=214,
  serialized_end=261,
)

_PUTOBJECTREQUEST = _descriptor.Descriptor(
  name='PutObjectRequest',
  full_name='s3client.PutObjectRequest',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='object_name', full_name='s3client.PutObjectRequest.object_name', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='bucket_name', full_name='s3client.PutObjectRequest.bucket_name', index=1,
      number=2, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='x_amz_meta', full_name='s3client.PutObjectRequest.x_amz_meta', index=2,
      number=3, type=11, cpp_type=10, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='content', full_name='s3client.PutObjectRequest.content', index=3,
      number=4, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
  ],
  extensions=[
  ],
  nested_types=[_PUTOBJECTREQUEST_XAMZMETAENTRY, ],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=73,
  serialized_end=261,
)


_UPDATETAGSREQUEST_XAMZMETAENTRY = _descriptor.Descriptor(
  name='XAmzMetaEntry',
  full_name='s3client.UpdateTagsRequest.XAmzMetaEntry',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='key', full_name='s3client.UpdateTagsRequest.XAmzMetaEntry.key', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='value', full_name='s3client.UpdateTagsRequest.XAmzMetaEntry.value', index=1,
      number=2, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=_b('8\001'),
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=214,
  serialized_end=261,
)

_UPDATETAGSREQUEST = _descriptor.Descriptor(
  name='UpdateTagsRequest',
  full_name='s3client.UpdateTagsRequest',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='object_name', full_name='s3client.UpdateTagsRequest.object_name', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='bucket_name', full_name='s3client.UpdateTagsRequest.bucket_name', index=1,
      number=2, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='x_amz_meta', full_name='s3client.UpdateTagsRequest.x_amz_meta', index=2,
      number=3, type=11, cpp_type=10, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
  ],
  extensions=[
  ],
  nested_types=[_UPDATETAGSREQUEST_XAMZMETAENTRY, ],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=264,
  serialized_end=437,
)


_REPLY = _descriptor.Descriptor(
  name='Reply',
  full_name='s3client.Reply',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='reply', full_name='s3client.Reply.reply', index=0,
      number=1, type=5, cpp_type=1, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=439,
  serialized_end=461,
)

_PUTOBJECTREQUEST_XAMZMETAENTRY.containing_type = _PUTOBJECTREQUEST
_PUTOBJECTREQUEST.fields_by_name['x_amz_meta'].message_type = _PUTOBJECTREQUEST_XAMZMETAENTRY
_UPDATETAGSREQUEST_XAMZMETAENTRY.containing_type = _UPDATETAGSREQUEST
_UPDATETAGSREQUEST.fields_by_name['x_amz_meta'].message_type = _UPDATETAGSREQUEST_XAMZMETAENTRY
DESCRIPTOR.message_types_by_name['CreateBucketRequest'] = _CREATEBUCKETREQUEST
DESCRIPTOR.message_types_by_name['PutObjectRequest'] = _PUTOBJECTREQUEST
DESCRIPTOR.message_types_by_name['UpdateTagsRequest'] = _UPDATETAGSREQUEST
DESCRIPTOR.message_types_by_name['Reply'] = _REPLY
_sym_db.RegisterFileDescriptor(DESCRIPTOR)

CreateBucketRequest = _reflection.GeneratedProtocolMessageType('CreateBucketRequest', (_message.Message,), {
  'DESCRIPTOR' : _CREATEBUCKETREQUEST,
  '__module__' : 's3client_pb2'
  # @@protoc_insertion_point(class_scope:s3client.CreateBucketRequest)
  })
_sym_db.RegisterMessage(CreateBucketRequest)

PutObjectRequest = _reflection.GeneratedProtocolMessageType('PutObjectRequest', (_message.Message,), {

  'XAmzMetaEntry' : _reflection.GeneratedProtocolMessageType('XAmzMetaEntry', (_message.Message,), {
    'DESCRIPTOR' : _PUTOBJECTREQUEST_XAMZMETAENTRY,
    '__module__' : 's3client_pb2'
    # @@protoc_insertion_point(class_scope:s3client.PutObjectRequest.XAmzMetaEntry)
    })
  ,
  'DESCRIPTOR' : _PUTOBJECTREQUEST,
  '__module__' : 's3client_pb2'
  # @@protoc_insertion_point(class_scope:s3client.PutObjectRequest)
  })
_sym_db.RegisterMessage(PutObjectRequest)
_sym_db.RegisterMessage(PutObjectRequest.XAmzMetaEntry)

UpdateTagsRequest = _reflection.GeneratedProtocolMessageType('UpdateTagsRequest', (_message.Message,), {

  'XAmzMetaEntry' : _reflection.GeneratedProtocolMessageType('XAmzMetaEntry', (_message.Message,), {
    'DESCRIPTOR' : _UPDATETAGSREQUEST_XAMZMETAENTRY,
    '__module__' : 's3client_pb2'
    # @@protoc_insertion_point(class_scope:s3client.UpdateTagsRequest.XAmzMetaEntry)
    })
  ,
  'DESCRIPTOR' : _UPDATETAGSREQUEST,
  '__module__' : 's3client_pb2'
  # @@protoc_insertion_point(class_scope:s3client.UpdateTagsRequest)
  })
_sym_db.RegisterMessage(UpdateTagsRequest)
_sym_db.RegisterMessage(UpdateTagsRequest.XAmzMetaEntry)

Reply = _reflection.GeneratedProtocolMessageType('Reply', (_message.Message,), {
  'DESCRIPTOR' : _REPLY,
  '__module__' : 's3client_pb2'
  # @@protoc_insertion_point(class_scope:s3client.Reply)
  })
_sym_db.RegisterMessage(Reply)


DESCRIPTOR._options = None
_PUTOBJECTREQUEST_XAMZMETAENTRY._options = None
_UPDATETAGSREQUEST_XAMZMETAENTRY._options = None

_S3 = _descriptor.ServiceDescriptor(
  name='S3',
  full_name='s3client.S3',
  file=DESCRIPTOR,
  index=0,
  serialized_options=None,
  serialized_start=464,
  serialized_end=656,
  methods=[
  _descriptor.MethodDescriptor(
    name='CreateBucket',
    full_name='s3client.S3.CreateBucket',
    index=0,
    containing_service=None,
    input_type=_CREATEBUCKETREQUEST,
    output_type=_REPLY,
    serialized_options=None,
  ),
  _descriptor.MethodDescriptor(
    name='PutObject',
    full_name='s3client.S3.PutObject',
    index=1,
    containing_service=None,
    input_type=_PUTOBJECTREQUEST,
    output_type=_REPLY,
    serialized_options=None,
  ),
  _descriptor.MethodDescriptor(
    name='UpdateTags',
    full_name='s3client.S3.UpdateTags',
    index=2,
    containing_service=None,
    input_type=_UPDATETAGSREQUEST,
    output_type=_REPLY,
    serialized_options=None,
  ),
])
_sym_db.RegisterServiceDescriptor(_S3)

DESCRIPTOR.services_by_name['S3'] = _S3

# @@protoc_insertion_point(module_scope)