
class Properties(object):

    def __init__(self, fileName):
        self.fileName = fileName
        self.properties = {}

    def __getDict(self, strName, dictName, value):

        if strName.find('.') > 0:
            k = strName.split('.')[0]
            dictName.setdefault(k, {})
            return self.__getDict(strName[len(k) + 1:], dictName[k], value)
        else:
            dictName[strName] = value
            return

    def getProperties(self):
        try:
            pro_file = open(self.fileName, 'Ur')
            for line in pro_file.readlines():
                line = line.strip().replace('\n', '')
                if line.find("#") != -1:
                    line = line[0:line.find('#')]
                if line.find('=') > 0:
                    strs = line.split('=')
                    strs[1] = line[len(strs[0]) + 1:]
                    self.__getDict(strs[0].strip(), self.properties, strs[1].strip())
        except Exception as e:
            raise e
        else:
            pro_file.close()
        return self.properties


"""
下面我们再看一下具体的用法，首先在项目中增加如下模块引入
#引入解析Properties的模块
from ReadProperties import Propertiespy

接着我们再创建properties配置文件，我的测试文件如下所示：
www.alibaba.com=tianmao
chat=weixin,qq
shopping=jingdong

使用方法如下：
  pro=Properties('company.properties').getProperties()  
  print pro
  print pro['www']
  print pro['www']['alibaba']
  print pro['chat']

输出结果如下：
{'www': {'alibaba': {'com': 'tianmao'}}, 'shopping': 'jingdong', 'chat': 'weixin,qq'}

{'alibaba': {'com': 'tianmao'}}

{'com': 'tianmao'}

weixin,qq


链接：https://www.jianshu.com/p/395fe8c4e8af
本文参考博客：http://blog.csdn.net/bobzhangshaobo/article/details/47617107
https://www.jb51.net/article/137393.htm

"""