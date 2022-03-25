import traceback
import logging

def select(entity, session, column, condition):
    try:
        if condition is None: # Checking if the variable is None
            return session.query(entity).all()
        else:
            return session.query(entity).filter(column == condition).all()
    except Exception as e:
        print(f"Exception not caught in Repository: {e}")
        print(traceback.format_exc())

def select2condition(entity, session, column1, condition1, column2, condition2):
    try:
        return session.query(entity).filter(column1 == condition1, column2 == condition2).all()
    except Exception as e:
        print(f"Exception not caught in Repository: {e}")
        print(traceback.format_exc())