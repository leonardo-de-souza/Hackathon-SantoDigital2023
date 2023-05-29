from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Relationship
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Float, ForeignKey, Date, Enum
import pandas as pd
import os

from database_url import DATABASE_URL

# Por enquanto, Tarefa1 precisa ser o diretório raiz do projeto, 
# se não haverá problemas ao localizar e ler os arquivos

if __name__ == '__main__':
    # Conectar ao banco de dados e iniciar uma sessão
    engine = create_engine(DATABASE_URL)

    Session = sessionmaker(bind=engine)
    session = Session()

    Base = declarative_base()

    # Tabela não utilizada
    # class Calendar(Base):
        #    __tablename__ = 'calendar'
        #    date = Column(Date)

    class Customer(Base):
        __tablename__ = 'customers'
        customerkey = Column(Integer, primary_key=True) # Contagem começa por 11.000
        prefix = Column(String(64))
        firstname = Column(String(64))
        lastname = Column(String(64))
        birthdate = Column(Date)
        maritalstatus = Column(String(4))
        gender = Column(String(4))
        emailaddress = Column(String(64))
        annualincome = Column(String(32)) # O valor no CSV vem com $
        totalchildren = Column(Integer)
        educationlevel = Column(String(64))
        occupation = Column(String(64))
        homeowner = Column(String(4))

    class ProductCategory(Base):
        __tablename__ = 'productcategories'
        productcategorykey = Column(Integer, primary_key=True, autoincrement="auto")
        categoryname = Column(String(64))

    class ProductSubcategory(Base):
        __tablename__ = 'productsubcategories'
        productsubcategorykey = Column(Integer, primary_key=True, autoincrement="auto")
        subcategoryname = Column(String(64))
        productcategorykey = Column(Integer, ForeignKey('productcategories.productcategorykey'))

    class Product(Base): # Alguns valores no CSV estão em colunas que não deveriam estar
        __tablename__ = 'products'
        productkey = Column(Integer, primary_key=True)
        productsubcategorykey = Column(Integer, ForeignKey('productsubcategories.productsubcategorykey'))
        productsku = Column(String(64))
        productname = Column(String(64))
        modelname = Column(String(64))
        productdescription = Column(String(256))
        productcolor = Column(String(64))
        productsize = Column(String(64))
        productstyle = Column(String(64))
        productcost = Column(Float)
        productprice = Column(Float)

    class Territory(Base):
        __tablename__ = 'territories'
        salesterritorykey = Column(Integer, primary_key=True)
        region = Column(String(64))
        country = Column(String(64))
        continent = Column(String(64))

    class Return(Base):
        __tablename__ = 'returns'
        returnkey = Column(Integer, primary_key=True)
        returndate = Column(Date)
        territorykey = Column(Integer, ForeignKey('territories.salesterritorykey'))
        productkey = Column(Integer, ForeignKey('products.productkey'))
        returnquantity = Column(Integer)  

    class Sales(Base):
        __tablename__ = 'sales'
        orderkey = Column(Integer, primary_key=True, autoincrement=True)
        orderdate = Column(Date)
        stockdate = Column(Date)
        ordernumber = Column(String(256)) # Remover caractere préfixo SO e transformar em int
        productkey = Column(Integer, ForeignKey('products.productkey'))
        customerkey = Column(Integer, ForeignKey('customers.customerkey'))
        territorykey = Column(Integer, ForeignKey('territories.salesterritorykey'))
        orderlineitem = Column(Integer)
        orderquantity = Column(Integer)

    # Criar tabelas
    Base.metadata.create_all(bind=engine)

    # data = pandas.read_csv('AdventureWorks/AdventureWorks_Calendar.csv', parse_dates=['Date'])
    # data.to_sql(con=engine, name=Calendar.__tablename__, if_exists='append', index=False)

    # Categories
    data = pd.read_csv('AdventureWorks/AdventureWorks_Product_Categories.csv')
    data.to_sql(con=engine, name=ProductCategory.__tablename__, if_exists='append', index=False)

    # Subcategories
    data = pd.read_csv('AdventureWorks/AdventureWorks_Product_Subcategories.csv')
    data.to_sql(con=engine, name=ProductSubcategory.__tablename__, if_exists='append', index=False)

    # Products
    data = pd.read_csv('AdventureWorks/AdventureWorks_Products.csv')
    data.to_sql(con=engine, name=Product.__tablename__, if_exists='append', index=False)

    # Problema com encoding e conversão de MM/DD/YY -> YY/MM/DD
    # Customers
    data = pd.read_csv('AdventureWorks/AdventureWorks_Customers.csv', encoding='latin-1', parse_dates=['BirthDate'])
    data.to_sql(con=engine, name=Customer.__tablename__, if_exists='append', index=False)

    # Territories
    data = pd.read_csv('AdventureWorks/AdventureWorks_Territories.csv')
    data.to_sql(con=engine, name=Territory.__tablename__, if_exists='append', index=False)

    # Returns
    data = pd.read_csv('AdventureWorks/AdventureWorks_Returns.csv', parse_dates=['ReturnDate'])
    data.to_sql(con=engine, name=Return.__tablename__, if_exists='append', index=False)

    # Sales
    data = pd.read_csv('AdventureWorks/AdventureWorks_Sales_2015.csv', parse_dates=['OrderDate', 'StockDate'])
    data.to_sql(con=engine, name=Sales.__tablename__, if_exists='append', index=False)

    data = pd.read_csv('AdventureWorks/AdventureWorks_Sales_2016.csv', parse_dates=['OrderDate', 'StockDate'])
    data.to_sql(con=engine, name=Sales.__tablename__, if_exists='append', index=False)

    data = pd.read_csv('AdventureWorks/AdventureWorks_Sales_2017.csv', parse_dates=['OrderDate', 'StockDate'])
    data.to_sql(con=engine, name=Sales.__tablename__, if_exists='append', index=False)