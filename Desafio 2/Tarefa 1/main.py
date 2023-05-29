from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from sqlalchemy import create_engine, delete, desc, func
from sqlalchemy.orm import sessionmaker, Query
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Float, ForeignKey, Date
from database_url import DATABASE_URL

app = FastAPI()

engine = create_engine(DATABASE_URL)

SessionLocal = sessionmaker(bind=engine)

Base = declarative_base()

# Alchemy model
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

class ProductSubcategory(Base):
    __tablename__ = 'productsubcategories'
    productsubcategorykey = Column(Integer, primary_key=True, autoincrement="auto")
    subcategoryname = Column(String(64))
    productcategorykey = Column(Integer, ForeignKey('productcategories.productcategorykey'))

class Product(Base):
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

class ProductCategory(Base):
    __tablename__ = 'productcategories'
    productcategorykey = Column(Integer, primary_key=True, autoincrement="auto")
    categoryname = Column(String(64))

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


# Pydanctic model
class ProductCreate(BaseModel):
    productsku: str
    productname: str
    modelname: str
    productdescription: str
    productcolor: str
    productsize: str
    productstyle: str
    productcost: float
    productprice: float

session = SessionLocal()

# Create
@app.post('/products')
async def create_product(product: ProductCreate):
    session = SessionLocal()
    session_product = Product(**product.dict())
    
    session.add(session_product)
    session.commit()
    
    return product

# Read
@app.get('/products')
async def get_products():
    try:
        result = session.query(Product).all()

    except SQLAlchemyError as e:
        session.rollback()
        print("product constrained:", e)

    return result

@app.get('/products/{id}')
async def get_product(id : int):
    try:
        session_product = session.query(Product).filter(Product.productkey == id).first()

    except SQLAlchemyError as e:
        session.rollback()
        print("product constrained:", e)

    if session_product is None:
        raise HTTPException(status_code=404, detail="product not found")

    return session_product

# Update
@app.put('/products/{id}')
async def put_product(id : int, product: ProductCreate):
    session_product = session.query(Product).filter(Product.productkey == id).first()
    try:
        session_product.productsku = product.productsku
        session_product.productname = product.productname
        session_product.modelname = product.modelname
        session_product.productdescription = product.productdescription
        session_product.productcolor = product.productcolor
        session_product.productsize = product.productsize
        session_product.productstyle = product.productstyle
        session_product.productcost = product.productcost
        session_product.productprice = product.productcost
        session.commit()
    except SQLAlchemyError as e:
        session.rollback()
        print("product constrained:", e)
    
    return session_product

# Delete
@app.delete('/products/{id}')
async def delete_product(id: int):  
    session_product = session.query(Product).filter(Product.productkey == id).first()

    if session_product is None:
        raise HTTPException(status_code=404)
    
    try:
        session.delete(session_product)
        session.commit()

    except SQLAlchemyError as e:
        session.rollback()
        print("product constrained:", e)
    
    return session_product

# Tarefa 3
# Rota 1
@app.get('/sales/top-products/category/{category}')
async def top_products(category: str):
    result = session.query(Product.productname, func.sum(Sales.orderquantity).label("total")) \
        .join(ProductSubcategory, Product.productsubcategorykey == ProductSubcategory.productsubcategorykey) \
        .join(ProductCategory, ProductSubcategory.productcategorykey == ProductCategory.productcategorykey) \
        .join(Sales, Product.productkey == Sales.productkey) \
        .filter(ProductCategory.categoryname == category) \
        .group_by(Product.productname) \
        .order_by(desc("total")) \
        .limit(10) \
        .all()

    result = [result[0] for result in result]

    return result

# Rota 2
@app.get('/sales/best-customer')
async def best_customer():
    result = session.query(Customer.firstname, Customer.lastname, func.count(Sales.ordernumber).label('total')) \
        .outerjoin(Sales, Customer.customerkey == Sales.customerkey) \
        .group_by(Customer.customerkey, Customer.firstname, Customer.lastname) \
        .order_by(func.count(Sales.ordernumber).desc()) \
        .limit(1)
    
    result = [result[0] for result in result]

    return result

