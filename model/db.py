import re
import logging
from itertools import groupby
from random import shuffle

from sqlalchemy import create_engine, Column, Integer, String, BigInteger, ForeignKey, or_
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

logger = logging.getLogger(__name__)
Base = declarative_base()


class AnnotationTaskBlock(Base):
    __tablename__ = 'annotation_task_block'

    id = Column(BigInteger, primary_key=True)
    annotation = Column(String)
    annotation_type = Column(Integer)
    state = Column(String)
    text = Column(String)


class AnnotationTaskDocBlock(Base):
    __tablename__ = 'annotation_task_doc_block'

    block_id = Column(BigInteger, ForeignKey('annotation_task_block.id'), primary_key=True)
    task_doc_id = Column(BigInteger, ForeignKey('annotation_task_doc.id'), primary_key=True)
    block_order = Column(Integer, primary_key=True)


class AnnotationTaskDoc(Base):
    __tablename__ = 'annotation_task_doc'

    id = Column(BigInteger, primary_key=True)
    annotation_type = Column(Integer)
    doc_id = Column(BigInteger)
    task_id = Column(BigInteger, ForeignKey('annotation_task.id'))
    state = Column(String)


class AnnotationTask(Base):
    __tablename__ = 'annotation_task'

    id = Column(BigInteger, primary_key=True)


def get_entity(anno):
    if not anno:
        return None

    if anno[0] == 'R':
        tabs = re.split('\s', anno)
        if len(tabs) != 4:
            logger.warning('wrong length "%s"', anno)
            return None

        return {
            'tag': tabs[0],
            'type': tabs[1],
            'source': tabs[2].split(':')[1],
            'target': tabs[3].split(':')[1],
        }

    if anno[0] == 'T':
        tabs = re.split('\s', anno, 4)
        if len(tabs) != 4 and len(tabs) != 5:
            logger.warning('wrong length "%s"', anno)
            return None

        return {
            'tag': tabs[0],
            'type': tabs[1].replace('-and', ''),
            'start': int(tabs[2]),
            'end': int(tabs[3]),
        }

    logger.warning('wrong entity "%s"', anno)


def write_block(block, f):
    _, _, _, text, annotation = block

    entities = list(filter(lambda entity: entity, [get_entity(anno) for anno in annotation.split('\n')]))
    entity = None
    for index, ch in enumerate(text):
        if entity and index != entity['end']:
            f.write('{0} I-{1}\n'.format(ch, entity['type']))
            continue

        entity = None
        for en in entities:
            if en.get('start', -1) == index:
                entity = en
                f.write('{0} B-{1}\n'.format(ch, entity['type']))
                break

        if not entity:
            f.write('{0} O\n'.format(ch))


def write_data(docs, filename):
    with open(filename, 'w') as f:
        for doc in docs:
            for block in doc:
                write_block(block, f)
                f.write('\n')


def prepare_data(config):
    engine = create_engine('mysql+mysqlconnector://{username}:{password}@{host}/{db_name}'.format_map(config.db), echo=True)
    session = sessionmaker(bind=engine)()
    data = session.query(
        AnnotationTaskBlock.id,
        AnnotationTaskDocBlock.block_order,
        AnnotationTaskDocBlock.task_doc_id,
        AnnotationTaskBlock.text,
        AnnotationTaskBlock.annotation,
    ).join(AnnotationTaskDocBlock).join(AnnotationTaskDoc).join(AnnotationTask).filter(
        AnnotationTask.id == 3,
        AnnotationTaskBlock.annotation_type == 2,
        AnnotationTaskBlock.state.in_(['ANNOTATED', 'FINISHED']),
    ).all()

    docs = []
    for task_doc_id, blocks in groupby(data, lambda d: d[2]):
        docs.append(sorted(blocks, key=lambda d: d[1]))

    shuffle(docs)
    bucket = int(len(docs) / 10)

    write_data(docs[:8*bucket], config.filename_train)
    write_data(docs[8*bucket:9*bucket], config.filename_test)
    write_data(docs[9*bucket:], config.filename_dev)
