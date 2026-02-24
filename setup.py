import setuptools

setuptools.setup(
    name='my-custom-transforms',
    version='1.0',
    # This line tells Beam: "Please zip up my local transforms.py file and send it to Dataflow!"
    py_modules=['transforms'] 
)